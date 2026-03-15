use anyhow::{Result, bail};
use disk_config::DiskSimulationConfig;
use std::io::{BufRead, Read, Write};

use anyhow::Context;

use crate::disk_simulation::{
    disk::Disk,
    disk_io_metrics::{DiskIOMetricsResult, DiskIOMetricsSimulator},
};

pub struct DiskSimulator<R: BufRead, W: Write> {
    disk: Disk,
    disk_io_metrics_simulator: DiskIOMetricsSimulator,
    input: R,
    output: W,
    cache_block: Vec<u8>,
}

const MAX_COMMAND_LENGTH: u64 = 1024;

impl<R: BufRead, W: Write> DiskSimulator<R, W> {
    pub fn new(disk_simulation_config: DiskSimulationConfig, reader: R, writer: W) -> Self {
        let block_size = disk_simulation_config.get_disk_config().block_size;
        DiskSimulator {
            disk: Disk::new(
                disk_simulation_config.get_disk_config(),
                disk_simulation_config.get_files_spec(),
            ),
            disk_io_metrics_simulator: DiskIOMetricsSimulator::new(
                disk_simulation_config.get_disk_config().clone(),
            ),
            input: reader,
            output: writer,
            cache_block: vec![0u8; block_size as usize],
        }
    }

    pub fn simulate(&mut self) -> Result<DiskIOMetricsResult> {
        loop {
            let command = self.read_tokens().context("Failed to read tokens")?;

            if command.is_empty() {
                return Ok(self.disk_io_metrics_simulator.get_current_metrics());
            }

            let result = match command[0].to_lowercase().as_ref() {
                "exit" => return Ok(self.disk_io_metrics_simulator.get_current_metrics()),
                "get" => self.handle_get_command(&command[1..]),
                "put" => self.handle_put_command(&command[1..]),
                other => bail!("Unknown command: {other}"),
            };

            result.context(String::from(&command[0]))?;
            self.output.flush()?; // flush out any output written by disk due to any command
        }
    }

    fn handle_get_command(&mut self, command: &[String]) -> Result<()> {
        if command.is_empty() {
            bail!("get command requires more arguments");
        }

        let result = match command[0].to_lowercase().as_ref() {
            "block" => self.handle_get_block(&command[1..]),
            "block-size" => self.handle_get_block_size(&command[1..]),
            "file" => self.handle_get_file_command(&command[1..]),
            "anon-start-block" => self.handle_get_anon_start_block(&command[1..]),
            other => bail!("Unknown command: {other}"),
        };

        result.context("get")?;
        Ok(())
    }

    fn handle_put_command(&mut self, command: &[String]) -> Result<()> {
        if command.is_empty() {
            bail!("put command needs more arguments");
        }

        let result = match command[0].to_lowercase().as_ref() {
            "block" => self.handle_put_block(&command[1..]),
            other => bail!("Unknown command: {other}"),
        };

        result.context("put")?;
        Ok(())
    }

    fn handle_put_block(&mut self, command: &[String]) -> Result<()> {
        if command.len() != 2 {
            bail!("block command needs exactly 2 arguments");
        }

        let start_block_id: u64 = command[0]
            .parse()
            .context(format!("Failed to parse start block id: {}", command[0]))?;

        let num_blocks: u64 = command[1]
            .parse()
            .context(format!("Failed to parse num_blocks: {}", command[1]))?;

        for offset in 0..num_blocks {
            self.input.read_exact(&mut self.cache_block)?;
            self.disk
                .write_block(start_block_id + offset, &self.cache_block)?;
        }

        self.disk_io_metrics_simulator
            .update_write_on(start_block_id, num_blocks);

        Ok(())
    }

    fn handle_get_block(&mut self, command: &[String]) -> Result<()> {
        if command.len() != 2 {
            bail!("block command needs exactly 2 arguments");
        }

        let start_block_id: u64 = command[0]
            .parse()
            .context(format!("Failed to parse start block id: {}", command[0]))?;

        let num_blocks: u64 = command[1]
            .parse()
            .context(format!("Failed to parse num_blocks: {}", command[1]))?;

        for offset in 0..num_blocks {
            self.disk
                .read_block(start_block_id + offset, &mut self.cache_block)?;
            self.output.write_all(&self.cache_block)?;
        }

        self.disk_io_metrics_simulator
            .update_read_on(start_block_id, num_blocks);

        Ok(())
    }

    fn handle_get_file_command(&mut self, command: &[String]) -> Result<()> {
        if command.is_empty() {
            bail!("file command requires more arguments");
        }

        match command[0].to_lowercase().as_ref() {
            "start-block" => {
                if command.len() != 2 {
                    bail!("file start_block_id requires exactly one additional argument");
                }
                self.output.write_all(
                    format!(
                        "{}\n",
                        self.disk
                            .get_file_start_block(&command[1])
                            .context("Request file_id not exit")?
                    )
                    .as_bytes(),
                )?
            }
            "num-blocks" => {
                if command.len() != 2 {
                    bail!("file num_blocks requires exactly one additional argument");
                }
                self.output.write_all(
                    format!(
                        "{}\n",
                        self.disk
                            .get_num_file_blocks(&command[1])
                            .context("Request file_id not exit")?
                    )
                    .as_bytes(),
                )?
            }
            other => bail!("Unknown file command: {other}"),
        }

        Ok(())
    }

    fn handle_get_block_size(&mut self, command: &[String]) -> Result<()> {
        if !command.is_empty() {
            bail!("block size requires no arguments");
        }
        self.output
            .write_all(format!("{}\n", self.disk.get_block_size()).as_bytes())?;

        Ok(())
    }

    fn handle_get_anon_start_block(&mut self, command: &[String]) -> Result<()> {
        if !command.is_empty() {
            bail!("anon start block requires no arguments");
        }
        self.output
            .write_all(format!("{}\n", self.disk.get_anon_start_block()).as_bytes())?;

        Ok(())
    }

    fn read_tokens(&mut self) -> Result<Vec<String>> {
        loop {
            let mut input_line = String::new();
            let mut limited_reader = (&mut self.input).take(MAX_COMMAND_LENGTH);
            let read_length = limited_reader
                .read_line(&mut input_line)
                .context("Failed to read input line")?;

            let result: Vec<String> = input_line.split_whitespace().map(String::from).collect();

            if result.len() > 0 || read_length == 0 {
                return Ok(result);
            }
        }
    }
}
