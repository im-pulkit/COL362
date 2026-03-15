use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use common::query::Query;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct MonitorConfig {
    disk_config: DiskConfig,
    database_config: DatabaseConfig,
    query_configs: Vec<QueryConfig>,
}

#[derive(Deserialize, Serialize)]
pub struct QueryConfig {
    pub execution_name: String,
    #[serde(default)]
    pub disabled: bool,
    pub query: Query,
    pub expected_output_file: PathBuf,
    pub memory_limit_mb: u64,
}

#[derive(Deserialize, Serialize)]
pub struct DiskConfig {
    pub disk_prog: PathBuf,
    pub disk_prog_config: PathBuf,
}

#[derive(Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub database_prog: PathBuf,
    pub database_prog_config: PathBuf,
}

impl MonitorConfig {
    pub fn from(
        disk_config: DiskConfig,
        database_config: DatabaseConfig,
        query_configs: Vec<QueryConfig>,
    ) -> Result<MonitorConfig> {
        let monitor_config = Self {
            disk_config,
            database_config,
            query_configs,
        };

        Self::validate_config(&monitor_config)?;
        Ok(monitor_config)
    }

    pub fn load_config(config_path: &PathBuf) -> Result<MonitorConfig> {
        let monitor_config: MonitorConfig = serde_json::from_str(&fs::read_to_string(config_path)?)
            .context("Unable to parse config")?;

        Self::validate_config(&monitor_config).context("Config is invalid")?;

        Ok(monitor_config)
    }

    fn validate_config(monitor_config: &MonitorConfig) -> Result<()> {
        Self::validate_file_exist(&monitor_config.disk_config.disk_prog)?;
        Self::validate_file_exist(&monitor_config.disk_config.disk_prog_config)?;

        Self::validate_file_exist(&monitor_config.database_config.database_prog)?;
        Self::validate_file_exist(&monitor_config.database_config.database_prog_config)?;

        for query_config in &monitor_config.query_configs {
            Self::validate_file_exist(&query_config.expected_output_file)?;

            if query_config.memory_limit_mb < 64 {
                bail!(
                    "Memory limit must be atleast 64 MB but found {}",
                    query_config.memory_limit_mb
                );
            }
        }

        Ok(())
    }

    fn validate_file_exist(file: &PathBuf) -> Result<()> {
        match fs::exists(&file)? {
            true => Ok(()),
            false => bail!("File {} doesn't exitst", file.to_string_lossy()),
        }
    }

    pub fn get_disk_config(&self) -> &DiskConfig {
        &self.disk_config
    }

    pub fn get_database_config(&self) -> &DatabaseConfig {
        &self.database_config
    }

    pub fn get_query_configs(&self) -> &[QueryConfig] {
        &self.query_configs
    }
}
