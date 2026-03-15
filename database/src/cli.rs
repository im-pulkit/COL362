use std::path::PathBuf;

#[derive(clap::Parser, Debug)]
pub struct CliOptions {
    #[arg(short, long)]
    config: PathBuf,
}

impl CliOptions {
    pub fn get_config_path(&self) -> &PathBuf {
        &self.config
    }
}
