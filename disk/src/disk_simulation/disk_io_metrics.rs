use disk_config::disk_simulation_config::DiskConfig;

pub struct DiskIOMetricsSimulator {
    disk_config: DiskConfig,
    current_cylinder: u64,
    last_end_block: u64,
    total_reads: u64,
    total_writes: u64,
    total_io_time_ns: u64,
    total_seek_time_ns: u64,
    total_rotational_latency_ns: u64,
    total_transfer_time_ns: u64,
    total_cylinders_traveled: u64,
    total_blocks_processed: u64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DiskIOMetricsResult {
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_blocks_processed: u64,
    pub total_cylinders_traveled: u64,
    pub total_io_time_us: u64,
    pub total_seek_time_us: u64,
    pub total_rotational_latency_us: u64,
    pub total_transfer_time_us: u64,
}

/// A simple IO metric simulatior, assumes same read and write speeds
impl DiskIOMetricsSimulator {
    pub fn new(disk_config: DiskConfig) -> Self {
        Self {
            disk_config,
            current_cylinder: 0,
            last_end_block: 0,
            total_reads: 0,
            total_writes: 0,
            total_io_time_ns: 0,
            total_seek_time_ns: 0,
            total_rotational_latency_ns: 0,
            total_transfer_time_ns: 0,
            total_cylinders_traveled: 0,
            total_blocks_processed: 0,
        }
    }

    pub fn update_read_on(&mut self, start_block: u64, num_blocks: u64) {
        self.total_reads += 1;
        self.simulate_io_time(start_block, num_blocks);
    }

    pub fn update_write_on(&mut self, start_block: u64, num_blocks: u64) {
        self.total_writes += 1;
        self.simulate_io_time(start_block, num_blocks);
    }

    pub fn get_current_metrics(&self) -> DiskIOMetricsResult {
        DiskIOMetricsResult {
            total_reads: self.total_reads,
            total_writes: self.total_writes,
            total_blocks_processed: self.total_blocks_processed,
            total_cylinders_traveled: self.total_cylinders_traveled,
            total_io_time_us: (self.total_io_time_ns + 500) / 1_000,
            total_seek_time_us: (self.total_seek_time_ns + 500) / 1_000,
            total_rotational_latency_us: (self.total_rotational_latency_ns + 500) / 1_000,
            total_transfer_time_us: (self.total_transfer_time_ns + 500) / 1_000,
        }
    }

    fn simulate_io_time(&mut self, start_block: u64, num_blocks: u64) {
        if num_blocks == 0 {
            return;
        }

        let blocks_per_cylinder =
            self.disk_config.blocks_per_track * self.disk_config.heads_per_cylinder;

        let end_block = start_block + num_blocks - 1;

        let start_cylinder = start_block / blocks_per_cylinder;
        let end_cylinder = end_block / blocks_per_cylinder;
        let distance = self.current_cylinder.abs_diff(start_cylinder);
        let cylinders_crossed = end_cylinder.abs_diff(start_cylinder);

        // Cap the distance ratio at 1.0 so seek times don't grow to infinity
        let distance_ratio =
            ((distance as f64) / (self.disk_config.total_cylinders as f64)).min(1.0);

        let seek_time_ms = if distance == 0 {
            0.0
        } else if distance == 1 {
            self.disk_config.track_to_track_seek_ms
        } else {
            self.disk_config.track_to_track_seek_ms
                + (self.disk_config.full_stroke_seek_ms - self.disk_config.track_to_track_seek_ms)
                    * distance_ratio.sqrt()
        };

        let is_sequential = start_block == self.last_end_block + 1;
        let rot_latency_ms = if is_sequential {
            0.0
        } else {
            (0.5 * 60.0 / (self.disk_config.rpm as f64)) * 1000.0
        };

        let total_bytes = num_blocks * self.disk_config.block_size;
        let total_mb = (total_bytes as f64) / 1_000_000.0;
        let transfer_time_ms = (total_mb / (self.disk_config.transfer_rate_mb_s as f64)) * 1000.0;

        let crossing_penalty_ms =
            (cylinders_crossed as f64) * self.disk_config.track_to_track_seek_ms;

        let seek_ns = ((seek_time_ms + crossing_penalty_ms) * 1_000_000.0).round() as u64;
        let rot_ns = (rot_latency_ms * 1_000_000.0).round() as u64;
        let transfer_ns = (transfer_time_ms * 1_000_000.0).round() as u64;

        self.total_blocks_processed += num_blocks;
        self.total_cylinders_traveled += distance + cylinders_crossed;

        self.total_seek_time_ns += seek_ns;
        self.total_rotational_latency_ns += rot_ns;
        self.total_transfer_time_ns += transfer_ns;

        self.total_io_time_ns += seek_ns + rot_ns + transfer_ns;

        self.current_cylinder = end_cylinder;
        self.last_end_block = end_block;
    }
}
