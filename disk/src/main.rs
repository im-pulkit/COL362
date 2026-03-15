use std::{
    io::{stdin, stdout},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use disk_config::DiskSimulationConfig;

use crate::{cli::CliOptions, disk_simulation::DiskSimulator};

mod cli;
mod disk_simulation;

fn disk_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let disk_simulation_config =
        DiskSimulationConfig::load_disk_simulation_config(cli_options.get_config_path())
            .context("Failed to load disk simulation config")?;

    let mut disk_simulator =
        DiskSimulator::new(disk_simulation_config, stdin().lock(), stdout().lock());

    let disk_io_metrics = disk_simulator.simulate()?;
    // Sleep some time expecting any output of db process would get flushed first
    std::thread::sleep(Duration::from_millis(100));
    eprintln!("--------------------------------------------------------------------------------");
    eprintln!("Disk IO metrics {:#?}", disk_io_metrics);
    Ok(())
}

fn main() -> Result<()> {
    disk_main().with_context(|| "From Disk")
}
