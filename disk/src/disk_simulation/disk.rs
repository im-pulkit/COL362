use std::{
    cmp::max,
    collections::HashMap,
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};

use disk_config::disk_simulation_config::{DiskConfig, FileSpec};

struct Block {
    data: Vec<u8>,
}

impl Block {
    fn new(size: usize) -> Block {
        Self {
            data: vec![0u8; size],
        }
    }
}

struct FileDiskMetadata {
    id: String,
    file_path: PathBuf,
    start_block: u64,
    num_blocks: u64,
}

pub struct Disk {
    block_size: u64, // In bytes
    anon_pages: HashMap<u64, Block>,
    anon_start_index: u64,
    file_metadata_mapping: HashMap<String, FileDiskMetadata>,
    open_files: HashMap<String, File>,
}

impl Disk {
    pub fn new(disk_config: &DiskConfig, files_config: &[FileSpec]) -> Disk {
        let file_metadata_mapping =
            Self::generate_file_metadata_mapping(disk_config.block_size, files_config.iter());
        let anon_start_index = Self::compute_anon_start_index(
            disk_config.block_size,
            file_metadata_mapping.iter().map(|(_key, value)| value),
        );
        Self {
            block_size: disk_config.block_size,
            anon_pages: HashMap::new(),
            anon_start_index: anon_start_index,
            file_metadata_mapping: file_metadata_mapping,
            open_files: HashMap::new(),
        }
    }

    pub fn get_file_start_block(&self, query_file_id: &str) -> Option<u64> {
        self.file_metadata_mapping
            .get(query_file_id)
            .map(|file_metadata| file_metadata.start_block)
    }

    pub fn get_num_file_blocks(&self, query_file_id: &str) -> Option<u64> {
        self.file_metadata_mapping
            .get(query_file_id)
            .map(|file_metadata| file_metadata.num_blocks)
    }

    pub fn get_block_size(&self) -> u64 {
        self.block_size
    }

    pub fn get_anon_start_block(&self) -> u64 {
        self.anon_start_index
    }

    pub fn read_block(&mut self, block_id: u64, buf: &mut [u8]) -> Result<()> {
        if buf.len() as u64 != self.block_size {
            bail!(
                "Read buffer size {} doesn't match the size of block {}",
                buf.len(),
                self.block_size
            )
        }

        if block_id >= self.anon_start_index {
            let block = self
                .anon_pages
                .get(&block_id)
                .context(format!("Block {} doesn't exist", block_id))?;

            buf.copy_from_slice(&block.data);
        } else {
            self.read_file_block(block_id, buf)?;
        }

        Ok(())
    }

    pub fn write_block(&mut self, block_id: u64, data: &[u8]) -> Result<()> {
        if block_id < self.anon_start_index {
            bail!(
                "Blocks below block_id: {} are write protected",
                self.anon_start_index
            );
        }

        if data.len() as u64 != self.block_size {
            bail!(
                "Size of data {} doesn't match block size {}",
                data.len(),
                self.block_size
            );
        }

        self.anon_pages
            .entry(block_id)
            .or_insert_with(|| Block::new(self.block_size as usize))
            .data
            .copy_from_slice(data);

        Ok(())
    }

    fn generate_file_metadata_mapping<'a>(
        block_size: u64,
        files_config: impl Iterator<Item = &'a FileSpec>,
    ) -> HashMap<String, FileDiskMetadata> {
        let mut file_metadata_mapping = HashMap::new();
        let mut current_position: u64 = 0;
        for file_config in files_config {
            let file_size = File::open(file_config.get_file_path())
                .expect(&format!(
                    "Failed to open file {}",
                    file_config.get_file_path().to_string_lossy()
                ))
                .metadata()
                .expect("Failed to read file metadata")
                .size();

            let num_file_blocks = file_size / block_size;

            file_metadata_mapping.insert(
                file_config.id.clone(),
                FileDiskMetadata {
                    id: file_config.id.clone(),
                    file_path: file_config.file_path.clone(),
                    start_block: current_position,
                    num_blocks: num_file_blocks,
                },
            );

            current_position += num_file_blocks
                + max(
                    4 * num_file_blocks,
                    (10_000_000_000 + block_size - 1) / block_size, // 10GB of gap at the least
                );
        }
        file_metadata_mapping
    }

    fn compute_anon_start_index<'a>(
        block_size: u64,
        file_disk_metadatas: impl Iterator<Item = &'a FileDiskMetadata>,
    ) -> u64 {
        let end_block_of_files = file_disk_metadatas
            .map(|fdma| fdma.start_block + fdma.num_blocks)
            .max()
            .unwrap_or(0);

        end_block_of_files + (10_000_000_000 + block_size - 1) / block_size
    }

    fn get_file_id(&self, block_id: u64) -> Option<&str> {
        for (_file_id, file_metadata) in &self.file_metadata_mapping {
            if block_id >= file_metadata.start_block
                && block_id < file_metadata.start_block + file_metadata.num_blocks
            {
                return Some(&file_metadata.id);
            }
        }
        None
    }

    fn get_file_from_id(&mut self, query_file_id: &str) -> &mut File {
        self.open_files
            .entry(query_file_id.to_string())
            .or_insert_with(|| {
                let file_metadata = self.file_metadata_mapping.get(query_file_id).unwrap();
                File::open(&file_metadata.file_path).unwrap()
            })
    }

    fn read_file_block(&mut self, block_id: u64, buf: &mut [u8]) -> Result<()> {
        let file_id = self
            .get_file_id(block_id)
            .context("Invalid file block id")?
            .to_string();
        let block_size = self.block_size;
        let start_block_id = self
            .file_metadata_mapping
            .get(&file_id)
            .unwrap()
            .start_block;

        let file = self.get_file_from_id(&file_id);

        file.read_exact_at(buf, (block_id - start_block_id) * block_size)?;
        Ok(())
    }
}
