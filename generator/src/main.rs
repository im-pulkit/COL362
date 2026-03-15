use anyhow::{Context, Result, bail};
use clap::Parser;
use common::{Data, DataType, query::QueryOp};
use db_config::{DbContext, table::TableSpec};
use disk_config::{
    DiskSimulationConfig,
    disk_simulation_config::{DiskConfig, FileSpec},
};
use monitor_config::{MonitorConfig, monitor_config::QueryConfig};

use std::{
    borrow::Borrow,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

use crate::{
    cli::{
        AllGenerationConfig, CliOptions, DatabaseGenerationConfig, DiskGenerationConfig,
        MonitorGenerationConfig, SqliteGenerationConfig,
    },
    stats_generator::StatsGenerator,
};

mod cli;
mod stats_generator;

struct ColumnSpec {
    column_name: String,
    data_type: DataType,
}

struct Row {
    elements: Vec<Data>,
}

fn find_row_size(row: &Row) -> usize {
    let mut size = 0;
    for element in &row.elements {
        size += match element {
            Data::Int32(_) => 4,
            Data::Int64(_) => 8,
            Data::Float32(_) => 4,
            Data::Float64(_) => 8,
            Data::String(data) => data.len() + 1,
        };
    }

    size
}

fn write_row(row: &Row, buf: &mut [u8]) {
    let mut current = buf;
    for element in &row.elements {
        let offset = match element {
            Data::Int32(value) => {
                current[..4].copy_from_slice(&value.to_le_bytes());
                4
            }
            Data::Int64(value) => {
                current[..8].copy_from_slice(&value.to_le_bytes());
                8
            }
            Data::Float32(value) => {
                current[..4].copy_from_slice(&value.to_le_bytes());
                4
            }
            Data::Float64(value) => {
                current[..8].copy_from_slice(&value.to_le_bytes());
                8
            }
            Data::String(value) => {
                current[..value.len()].copy_from_slice(&value.as_bytes());
                current[value.len()] = 0;
                value.len() + 1
            }
        };
        current = &mut current[offset..];
    }
}

fn save_row_stream_to_file<'a, T>(
    file_path: &PathBuf,
    row_stream: impl Iterator<Item = T>,
    block_size: usize,
) -> Result<()>
where
    T: Borrow<Row>,
{
    let file = File::create(file_path)?;

    let mut buf_writer = BufWriter::new(file);

    let mut buf = vec![0u8; block_size];

    let map_capacity = block_size - 2;
    let mut occupied_cpacity = 0;
    let mut current_row_count: u16 = 0;

    for row in row_stream {
        let row_size = find_row_size(row.borrow());

        if occupied_cpacity + row_size > map_capacity {
            buf[(block_size - 2)..block_size].copy_from_slice(&current_row_count.to_le_bytes());
            buf_writer.write_all(&buf)?;
            current_row_count = 0;
            occupied_cpacity = 0;
            buf.fill(0);
        }

        write_row(row.borrow(), &mut buf[occupied_cpacity..]);
        occupied_cpacity += row_size;
        current_row_count += 1;
    }

    if current_row_count > 0 {
        buf[(block_size - 2)..block_size].copy_from_slice(&current_row_count.to_le_bytes());
        buf_writer.write_all(&buf)?;
    }

    buf_writer.flush()?;
    Ok(())
}

struct Schema {
    column_specs: Vec<ColumnSpec>,
}

impl Schema {
    pub fn load_from_file(skm_file: &PathBuf) -> Result<Self> {
        let file = File::open(skm_file)?;
        let mut buf_reader = BufReader::new(file);

        let mut column_names_line = String::new();
        buf_reader.read_line(&mut column_names_line)?;

        let mut column_names_iter = column_names_line
            .trim()
            .trim_end_matches('|')
            .split('|')
            .map(String::from);

        let mut data_types_lines = String::new();
        buf_reader.read_line(&mut data_types_lines)?;
        let mut data_types_iter = data_types_lines
            .trim()
            .trim_end_matches('|')
            .split('|')
            .map(|t| match t.to_uppercase().as_ref() {
                "I32" => Ok(DataType::Int32),
                "I64" => Ok(DataType::Int64),
                "FLOAT32" => Ok(DataType::Float32),
                "FLOAT64" => Ok(DataType::Float64),
                "STRING" => Ok(DataType::String),
                other => bail!("Unknown data type {other}"),
            });

        let mut column_specs = Vec::new();

        loop {
            if let Some(column_name) = column_names_iter.next() {
                if let Some(data_type) = data_types_iter.next() {
                    column_specs.push(ColumnSpec {
                        column_name,
                        data_type: data_type?,
                    });
                } else {
                    bail!("Datatype not specified for column {}", column_name);
                }
            } else {
                break;
            }
        }

        Ok(Schema { column_specs })
    }
}

struct RowStreamGenerator {
    schema: Schema,
    tbl_reader: BufReader<File>,
}

impl RowStreamGenerator {
    pub fn new(tbl_file: &PathBuf, skm_file: &PathBuf) -> Result<RowStreamGenerator> {
        let tbl_reader = BufReader::new(File::open(tbl_file)?);

        let row_stream_generator = RowStreamGenerator {
            schema: Schema::load_from_file(&skm_file)?,
            tbl_reader,
        };

        Ok(row_stream_generator)
    }
}

impl Iterator for RowStreamGenerator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        if let Err(_) = self.tbl_reader.read_line(&mut line) {
            return None;
        }

        if line.trim().is_empty() {
            return None;
        }

        let elements: Vec<Data> = line
            .trim()
            .trim_end_matches('|')
            .split('|')
            .zip(self.schema.column_specs.iter())
            .map(|(value, column_spec)| match column_spec.data_type {
                DataType::Int32 => Data::Int32(value.parse().unwrap()),
                DataType::Int64 => Data::Int64(value.parse().unwrap()),
                DataType::Float32 => Data::Float32(value.parse().unwrap()),
                DataType::Float64 => Data::Float64(value.parse().unwrap()),
                DataType::String => Data::String(String::from(value)),
            })
            .collect();

        if elements.len() != self.schema.column_specs.len() {
            return None;
        }

        Some(Row { elements })
    }
}

struct TableConfig {
    table_name: String,
    table_file: PathBuf,
    schema_file: PathBuf,
}

fn create_bin_file(
    table_config: &TableConfig,
    output_path: &PathBuf,
    block_size: u64,
) -> Result<()> {
    let row_stream_generator =
        RowStreamGenerator::new(&table_config.table_file, &table_config.schema_file)?;

    save_row_stream_to_file(output_path, row_stream_generator, block_size as usize)?;
    Ok(())
}

fn create_disk_config(
    bin_files: &[PathBuf],
    config_file_path: &PathBuf,
    block_size: u64,
) -> Result<()> {
    let mut disk_config = DiskConfig::default();
    disk_config.block_size = block_size;

    let files_specs = bin_files
        .iter()
        .filter_map(|path| Some((path.file_stem()?.to_string_lossy(), path)))
        .map(|(file_stem, path)| FileSpec {
            id: String::from(file_stem),
            file_path: path.clone(),
        })
        .collect();
    let disk_simulation_config = DiskSimulationConfig::from(disk_config, files_specs)?;

    let mut config_file = File::create(config_file_path)?;
    let config_json = serde_json::to_string_pretty(&disk_simulation_config)?;
    config_file.write_all(config_json.as_bytes())?;

    Ok(())
}

fn get_table_configs_from_path(dataset_folder: &PathBuf) -> Result<Vec<TableConfig>> {
    let table_configs: Vec<TableConfig> = fs::read_dir(dataset_folder)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "csv"))
        .filter_map(|entry| {
            if entry.path().with_extension("schema").exists() {
                Some(TableConfig {
                    table_name: String::from(entry.path().file_stem()?.to_string_lossy()),
                    table_file: entry.path(),
                    schema_file: entry.path().with_extension("schema"),
                })
            } else {
                eprintln!(
                    "Skipping file {} due to missing schema file",
                    entry.path().to_string_lossy()
                );
                None
            }
        })
        .collect();
    Ok(table_configs)
}

fn handle_disk_generation(disk_generation_config: &DiskGenerationConfig) -> Result<()> {
    if !fs::exists(&disk_generation_config.dataset_folder)? {
        bail!(
            "dataset folder {} doesn't exist",
            &disk_generation_config.dataset_folder.to_string_lossy()
        );
    }

    fs::create_dir_all(&disk_generation_config.compiled_dataset_folder)?;
    fs::create_dir_all(&disk_generation_config.runtime_folder)?;

    let table_configs: Vec<TableConfig> =
        get_table_configs_from_path(&disk_generation_config.dataset_folder)?;

    let mut bin_files = Vec::new();
    for table_config in &table_configs {
        let mut output_path = disk_generation_config
            .compiled_dataset_folder
            .canonicalize()?;
        output_path.push(&table_config.table_name);
        output_path = output_path.with_extension("bin");

        create_bin_file(
            table_config,
            &output_path,
            disk_generation_config.block_size,
        )?;
        bin_files.push(output_path);
    }

    let mut config_file_path = disk_generation_config.runtime_folder.canonicalize()?;
    config_file_path.push("disk_sim_config.json");
    create_disk_config(
        &bin_files,
        &config_file_path,
        disk_generation_config.block_size,
    )?;

    Ok(())
}

fn generate_db_config(database_generation_config: &DatabaseGenerationConfig) -> Result<()> {
    fs::create_dir_all(&database_generation_config.runtime_folder)?;

    let table_configs = get_table_configs_from_path(&database_generation_config.dataset_folder)?;

    let mut table_specs = Vec::new();
    for table_config in &table_configs {
        let mut column_specs = Vec::new();
        let schema = Schema::load_from_file(&table_config.schema_file)?;

        let mut stat_generators: Vec<StatsGenerator> = (0..schema.column_specs.len())
            .map(|_| StatsGenerator::new())
            .collect();
        let row_generator =
            RowStreamGenerator::new(&table_config.table_file, &table_config.schema_file)?;
        for row in row_generator {
            row.elements
                .into_iter()
                .zip(stat_generators.iter_mut())
                .for_each(|(element, stat_generator)| stat_generator.update(element));
        }

        schema
            .column_specs
            .into_iter()
            .zip(stat_generators.into_iter())
            .for_each(|(column_spec, stat_generator)| {
                column_specs.push(db_config::table::ColumnSpec {
                    column_name: column_spec.column_name,
                    data_type: column_spec.data_type,
                    stats: Some(stat_generator.build()),
                });
            });

        table_specs.push(TableSpec {
            name: table_config.table_name.clone(),
            file_id: table_config.table_name.clone(),
            column_specs: column_specs,
        });
    }

    let db_context = DbContext::from(table_specs)?;

    // Save the db config file
    let mut config_file_path = database_generation_config.runtime_folder.canonicalize()?;
    config_file_path.push("db_config.json");
    let mut config_file = File::create(config_file_path)?;
    let config_json = serde_json::to_string_pretty(&db_context)?;
    config_file.write_all(config_json.as_bytes())?;

    Ok(())
}

fn generate_sqlite(sqlite_generation_config: &SqliteGenerationConfig) -> Result<()> {
    fs::create_dir_all(&sqlite_generation_config.compiled_dataset_folder)?;

    let mut sqlite_path = sqlite_generation_config.compiled_dataset_folder.clone();
    sqlite_path.push("sqlite.db");
    if sqlite_path.exists() {
        fs::remove_file(&sqlite_path)?;
    }

    let sqlite_connection = rusqlite::Connection::open(sqlite_path)?;

    let table_configs: Vec<TableConfig> =
        get_table_configs_from_path(&sqlite_generation_config.dataset_folder)?;
    for table_config in &table_configs {
        let schema = Schema::load_from_file(&table_config.schema_file)?;
        let sqlite_column_spec = schema
            .column_specs
            .iter()
            .map(|column_spec| {
                format!(
                    "{} {}",
                    column_spec.column_name,
                    match column_spec.data_type {
                        DataType::Int32 => "INTEGER",
                        DataType::Int64 => "INTEGER",
                        DataType::Float32 => "REAL",
                        DataType::Float64 => "REAL",
                        DataType::String => "TEXT",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let create_table_query = format!(
            "CREATE TABLE {} ({});",
            table_config.table_name, sqlite_column_spec
        );
        sqlite_connection.execute(&create_table_query, [])?;

        let row_stream =
            RowStreamGenerator::new(&table_config.table_file, &table_config.schema_file)?;
        // Instert rows in sqlite
        let columns_separated_by_coma = schema
            .column_specs
            .iter()
            .map(|column_spec| format!("{}", column_spec.column_name))
            .collect::<Vec<_>>()
            .join(",");

        let mut batch_execution_string = String::with_capacity(1024 * 1024);
        let rows_per_batch = 1024;
        let mut current_num_rows = 0;
        for row in row_stream {
            let row_values_separated_by_coma = row
                .elements
                .iter()
                .map(|element| match element {
                    Data::Int32(value) => value.to_string(),
                    Data::Int64(value) => value.to_string(),
                    Data::Float32(value) => value.to_string(),
                    Data::Float64(value) => value.to_string(),
                    Data::String(value) => format!("\"{}\"", value),
                })
                .collect::<Vec<_>>()
                .join(",");
            if current_num_rows == 0 {
                batch_execution_string.push_str(&format!("({})", row_values_separated_by_coma));
            } else {
                batch_execution_string.push_str(&format!(",({})", row_values_separated_by_coma));
            }

            current_num_rows += 1;

            if current_num_rows >= rows_per_batch {
                let row_insert_statement = format!(
                    "INSERT into {} ({}) values {};",
                    &table_config.table_name, &columns_separated_by_coma, &batch_execution_string
                );
                sqlite_connection.execute(&row_insert_statement, [])?;
                current_num_rows = 0;
                batch_execution_string.clear();
            }
        }
        if current_num_rows > 0 {
            let row_insert_statement = format!(
                "INSERT into {} ({}) values {};",
                &table_config.table_name, &columns_separated_by_coma, &batch_execution_string
            );
            sqlite_connection.execute(&row_insert_statement, [])?;
        }
    }

    Ok(())
}

fn generate_monitor_config(monitor_generation_config: &MonitorGenerationConfig) -> Result<()> {
    fs::create_dir_all(&monitor_generation_config.runtime_folder)?;

    let mut disk_prog = monitor_generation_config.build_path.canonicalize()?;
    disk_prog.push("disk");

    let mut disk_prog_config = monitor_generation_config.runtime_folder.canonicalize()?;
    disk_prog_config.push("disk_sim_config.json");

    let disk_config = monitor_config::monitor_config::DiskConfig {
        disk_prog,
        disk_prog_config,
    };

    let mut database_prog = monitor_generation_config.build_path.canonicalize()?;
    database_prog.push("database");

    let mut database_prog_config = monitor_generation_config.runtime_folder.canonicalize()?;
    database_prog_config.push("db_config.json");

    let db_config = monitor_config::monitor_config::DatabaseConfig {
        database_prog,
        database_prog_config,
    };

    let mut explected_output_1_path = monitor_generation_config.runtime_folder.canonicalize()?;
    explected_output_1_path.push("expected_1.csv");
    if !fs::exists(&explected_output_1_path)? {
        File::create_new(&explected_output_1_path)?;
    }

    let mut explected_output_2_path = monitor_generation_config.runtime_folder.canonicalize()?;
    explected_output_2_path.push("expected_2.csv");
    if !fs::exists(&explected_output_2_path)? {
        File::create_new(&explected_output_2_path)?;
    }

    let query_config1 = QueryConfig {
        execution_name: String::from("Simple Scan"),
        disabled: true,
        query: QueryOp::scan("TableA").build(),
        expected_output_file: explected_output_1_path,
        memory_limit_mb: 64,
    };

    let query_config2 = QueryConfig {
        execution_name: String::from("Another Simple Scan"),
        disabled: true,
        query: QueryOp::scan("TableB").build(),
        expected_output_file: explected_output_2_path,
        memory_limit_mb: 64,
    };
    let monitor_config =
        MonitorConfig::from(disk_config, db_config, vec![query_config1, query_config2])?;

    // Save the monitor config file
    let mut config_file_path = monitor_generation_config.runtime_folder.canonicalize()?;
    config_file_path.push("monitor_config.json");
    let mut config_file = File::create(config_file_path)?;
    let config_json = serde_json::to_string_pretty(&monitor_config)?;
    config_file.write_all(config_json.as_bytes())?;

    Ok(())
}

fn generate_all(all_generation_config: &AllGenerationConfig) -> Result<()> {
    handle_disk_generation(&DiskGenerationConfig {
        dataset_folder: all_generation_config.dataset_folder.clone(),
        compiled_dataset_folder: all_generation_config.compiled_dataset_folder.clone(),
        runtime_folder: all_generation_config.runtime_folder.clone(),
        block_size: all_generation_config.block_size,
    })?;

    generate_db_config(&DatabaseGenerationConfig {
        dataset_folder: all_generation_config.dataset_folder.clone(),
        runtime_folder: all_generation_config.runtime_folder.clone(),
    })?;

    generate_monitor_config(&MonitorGenerationConfig {
        runtime_folder: all_generation_config.runtime_folder.clone(),
        build_path: all_generation_config.build_path.clone(),
    })?;

    generate_sqlite(&SqliteGenerationConfig {
        dataset_folder: all_generation_config.dataset_folder.clone(),
        compiled_dataset_folder: all_generation_config.compiled_dataset_folder.clone(),
    })?;
    Ok(())
}

fn generator_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    match cli_options {
        CliOptions::Disk(disk_generation_config) => {
            handle_disk_generation(&disk_generation_config)?
        }
        CliOptions::Database(database_generation_config) => {
            generate_db_config(&database_generation_config)?
        }
        CliOptions::Monitor(monitor_generation_config) => {
            generate_monitor_config(&monitor_generation_config)?
        }
        CliOptions::Sqlite(sqlite_generation_config) => generate_sqlite(&sqlite_generation_config)?,
        CliOptions::All(all_generation_config) => generate_all(&all_generation_config)?,
    };
    Ok(())
}

fn main() -> Result<()> {
    generator_main().with_context(|| "From Generator")
}
