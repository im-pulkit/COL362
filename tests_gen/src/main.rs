use anyhow::{Context, Result, bail};
use std::{
    fs::{self, File},
    io::{Write},
    path::PathBuf,
    process::Command,
};

use clap::Parser;
use common::query::Query;
use monitor_config::{MonitorConfig, monitor_config::QueryConfig};
use os_pipe::{pipe, PipeReader, PipeWriter};

mod tests;

#[derive(clap::Parser)]
pub struct CliOptions {
    #[clap(short, long)]
    pub compiled_dataset_folder: PathBuf,
    #[clap(short, long)]
    pub runtime_folder: PathBuf,
}

fn main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let mut monitor_config_path = cli_options.runtime_folder.clone();
    monitor_config_path.push("monitor_config.json");
    let mut monitor_config: MonitorConfig = serde_json::from_str(
        &fs::read_to_string(&monitor_config_path).context("Failed to read monitor config")?,
    )
    .context("Failed to parse monitor config")?;

    let tests = get_all_tests();

    let mut query_configs = Vec::new();
    let mut query_number = 0;
    for (query, sql_query,is_output_sorted) in tests {
        query_number += 1;
        println!("Processing query {}", sql_query);

        let mut sqlite_db_path = cli_options.compiled_dataset_folder.clone();
        sqlite_db_path.push("sqlite.db");
        let mut expected_output_path = cli_options.runtime_folder.clone();
        expected_output_path.push(&format!("expected_output_{}.csv", query_number));
        let exepected_output_file = File::create(expected_output_path.clone())?;

        let (pipe_reader, mut pipe_writer) = pipe()?;
        let mut sqlite_process = Command::new("sqlite3")
            .arg(&sqlite_db_path.to_string_lossy().to_string())
            .stdin(pipe_reader)
            .stdout(exepected_output_file)
            .spawn()
            .context("Failed to spawn sliqte3 process")?;
        pipe_writer
            .write_all(sql_query.as_bytes())
            .context("Failed to write to sqlite_process")?;
        drop(pipe_writer); // Otherwise process will hang forever
        
        let exist_status = sqlite_process
            .wait()
            .context("Failed to wait on sqlite3 process")?;
        if !exist_status.success() {
            bail!("Sqlite process didn't exit cleanly on query {}", sql_query);
        }

        query_configs.push(QueryConfig {
            execution_name: sql_query.clone(),
            disabled: false,
            sort_before_check: !is_output_sorted,
            query,
            expected_output_file: expected_output_path.canonicalize()?,
            memory_limit_mb: 64,
        });
    }

    monitor_config.query_configs = query_configs;
    fs::write(
        monitor_config_path,
        serde_json::to_string_pretty(&monitor_config)?,
    )?;
    Ok(())
}

fn get_all_tests() -> Vec<(Query, String,bool)> {
    let mut all_tests = get_all_correctness_tests();
    all_tests.extend(get_all_benchmark_tests());
    all_tests
}

fn get_all_correctness_tests() -> Vec<(Query, String,bool)> {
    vec![
        tests::test_q1(),
        tests::test_q2(),
        tests::test_q3(),
        tests::test_q4(),
        tests::test_q5(),
        tests::test_q6(),
        tests::test_q7(),
        tests::test_q8(),
        tests::test_q9(),
        tests::test_q10(),
        tests::test_q11(),
        tests::test_q12(),
        tests::test_q13(),
        tests::test_q14(),
        tests::test_q15(),
        tests::test_q16(),
        tests::test_q17(),
        tests::test_q18(),
        tests::test_q19(),
        tests::test_q20(),
        tests::test_q21(),
        tests::test_q22(),
        tests::test_q23(),
        tests::test_q24(),
        tests::test_q25(),
        tests::test_q26(),
        tests::test_q27(),
        tests::test_q28(),
        tests::test_q29(),
        tests::test_q30(),
        tests::test_q31(),
        tests::test_q32(),
        tests::test_q33(),
        tests::test_q34(),
        tests::test_q35(),
        tests::test_q36(),
        tests::test_q37(),
        tests::test_q38(),
        tests::test_q39(),
        tests::test_q40(),
        tests::test_q41(),
        tests::test_q42(),
        tests::test_q43(),
        tests::test_q44(),
        tests::test_q45(),
        tests::test_q46(),
        tests::test_q47(),
        tests::test_q48(),
        tests::test_q49(),
        tests::test_q50(),
    ]
}

fn get_all_benchmark_tests() -> Vec<(Query, String,bool)> {
    vec![
        tests::query_1(),
        tests::query_2(),
        tests::query_3(),
        tests::query_4(),
        tests::query_5(),
        tests::query_6(),
        tests::query_7(),
        tests::query_8(),
        tests::query_9(),
        tests::query_10(),
    ]
}
