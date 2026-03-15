use std::path::PathBuf;

#[derive(clap::Parser)]
pub struct DiskGenerationConfig {
    #[clap(short, long)]
    pub dataset_folder: PathBuf,
    #[clap(short, long)]
    pub compiled_dataset_folder: PathBuf,
    #[clap(short, long)]
    pub runtime_folder: PathBuf,
    #[clap(short = 's', long)]
    #[clap(default_value_t = 4096)]
    pub block_size: u64,
}

#[derive(clap::Parser)]
pub struct SqliteGenerationConfig {
    #[clap(short, long)]
    pub dataset_folder: PathBuf,
    #[clap(short, long)]
    pub compiled_dataset_folder: PathBuf,
}

#[derive(clap::Parser)]
pub struct DatabaseGenerationConfig {
    #[clap(short, long)]
    pub dataset_folder: PathBuf,
    #[clap(short, long)]
    pub runtime_folder: PathBuf,
}

#[derive(clap::Parser)]
pub struct MonitorGenerationConfig {
    #[clap(short, long)]
    pub runtime_folder: PathBuf,
    #[clap(short, long)]
    pub build_path: PathBuf,
}

#[derive(clap::Parser)]
pub struct AllGenerationConfig {
    #[clap(short, long)]
    pub dataset_folder: PathBuf,
    #[clap(short, long)]
    pub compiled_dataset_folder: PathBuf,
    #[clap(short, long)]
    pub runtime_folder: PathBuf,
    #[clap(short, long)]
    pub build_path: PathBuf,
    #[clap(short = 's', long)]
    #[clap(default_value_t = 4096)]
    pub block_size: u64,
}

#[derive(clap::Parser)]
pub enum CliOptions {
    /// Generates bin files and disk config
    Disk(DiskGenerationConfig),
    /// Generates db_config.json
    Database(DatabaseGenerationConfig),
    /// Generates monitor config
    Monitor(MonitorGenerationConfig),
    /// Generates sqlite database
    Sqlite(SqliteGenerationConfig),
    /// Generates all of the above
    All(AllGenerationConfig),
}
