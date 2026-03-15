use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::{fs, path::PathBuf};

use anyhow::Result;
use anyhow::bail;
use anyhow::{Context, Ok};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct DiskSimulationConfig {
    disk_config: DiskConfig,
    files: Vec<FileSpec>,
}

impl DiskSimulationConfig {
    pub fn from(disk_config: DiskConfig, files: Vec<FileSpec>) -> Result<Self> {
        let disk_simulation_config = Self { disk_config, files };
        Self::validate_disk_simulation_config(&disk_simulation_config)?;
        Ok(disk_simulation_config)
    }

    pub fn get_files_spec(&self) -> &Vec<FileSpec> {
        &self.files
    }

    pub fn get_disk_config(&self) -> &DiskConfig {
        &self.disk_config
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiskConfig {
    pub block_size: u64,
    pub rpm: f64,
    pub blocks_per_track: u64,
    pub heads_per_cylinder: u64,
    pub total_cylinders: u64,
    pub track_to_track_seek_ms: f64,
    pub full_stroke_seek_ms: f64,
    pub transfer_rate_mb_s: f64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            block_size: 4096,
            rpm: 7200.0,
            blocks_per_track: 1024,
            heads_per_cylinder: 4,
            total_cylinders: 500_000,
            track_to_track_seek_ms: 0.8,
            full_stroke_seek_ms: 18.0,
            transfer_rate_mb_s: 150.0,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileSpec {
    pub id: String,
    pub file_path: PathBuf,
}

impl FileSpec {
    pub fn get_file_path(&self) -> &PathBuf {
        &self.file_path
    }
}

impl DiskSimulationConfig {
    pub fn load_disk_simulation_config(
        disk_simulation_config_path: &PathBuf,
    ) -> Result<DiskSimulationConfig> {
        let data =
            fs::read_to_string(disk_simulation_config_path).context("Failed to read file")?;

        let disk_simulation_config: DiskSimulationConfig =
            serde_json::from_str(&data).context("Failed to parse file")?;

        Self::validate_disk_simulation_config(&disk_simulation_config)
            .context("Failed to validate disk simulation config")?;
        Ok(disk_simulation_config)
    }

    fn validate_disk_simulation_config(
        disk_simulation_config: &DiskSimulationConfig,
    ) -> Result<()> {
        let block_size = disk_simulation_config.disk_config.block_size;
        if block_size == 0 {
            bail!("Block size must be positive, but found {}", block_size);
        }

        for file in &disk_simulation_config.files {
            if file.id.contains(" ") {
                bail!("id can't contain space");
            }
            let file_size = File::open(&file.file_path)?.metadata()?.size();
            if file_size == 0 {
                bail!("File {} is empty", file.file_path.to_string_lossy());
            }
            if file_size % block_size != 0 {
                bail!(
                    "Size of file {} is not a multiple of block size {}",
                    file.file_path.to_string_lossy(),
                    block_size
                );
            }
        }

        Ok(())
    }
}
