use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use common::Data;
use db_config::statistics::{
    CardinalityData, ColumnStat, Density, Frequency, HistogramData, Range,
};
use rand::Rng;

#[derive(Debug, Clone)]
pub struct StatsConfig {
    /// The maximum number of items to keep in memory for the histogram sample.
    pub reservoir_capacity: usize,
    /// The maximum number of exact unique values to track before switching to HLL.
    pub exact_limit: usize,
    /// The precision parameter (p) for HyperLogLog. (e.g., p=12 uses 4KB of memory)
    pub hll_p: u8,
    /// The maximum number of buckets to divide the histogram into.
    pub max_histogram_buckets: usize,
    /// If a column is a String, only build a histogram if the unique count is less than or equal to this.
    /// High-cardinality strings (like UUIDs) produce useless range histograms.
    pub max_string_unique_for_histogram: u64,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            reservoir_capacity: 10_000,
            exact_limit: 10_000,
            hll_p: 12, // 4096 registers
            max_histogram_buckets: 10,
            // 50 unique strings across 10 buckets keeps the data comparable and useful
            max_string_unique_for_histogram: 50,
        }
    }
}

#[derive(Clone, Debug)]
struct OrderedData(Data);

impl PartialEq for OrderedData {
    fn eq(&self, other: &Self) -> bool {
        self.0.partial_cmp(&other.0) == Some(Ordering::Equal)
    }
}
impl Eq for OrderedData {}
impl PartialOrd for OrderedData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl Hash for OrderedData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Data::Int32(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            Data::Int64(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            Data::Float32(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Data::Float64(v) => {
                4u8.hash(state);
                v.to_bits().hash(state);
            }
            Data::String(v) => {
                5u8.hash(state);
                v.hash(state);
            }
        }
    }
}

pub struct HyperLogLog {
    registers: Vec<u8>,
    p: u8,
    m: usize,
    alpha: f64,
}

impl HyperLogLog {
    pub fn new(p: u8) -> Self {
        let m = 1 << p;
        let alpha = match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / (m as f64)),
        };
        Self {
            registers: vec![0; m],
            p,
            m,
            alpha,
        }
    }

    pub fn add<T: Hash>(&mut self, item: &T) {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();

        let index = (hash >> (64 - self.p)) as usize;
        let w = hash << self.p;
        let rank = (w.leading_zeros() + 1) as u8;

        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    pub fn count(&self) -> u64 {
        let mut sum = 0.0;
        for &v in &self.registers {
            sum += 2.0_f64.powi(-(v as i32));
        }
        let mut estimate = self.alpha * (self.m as f64) * (self.m as f64) / sum;

        if estimate <= 2.5 * (self.m as f64) {
            let zeros = self.registers.iter().filter(|&&v| v == 0).count();
            if zeros > 0 {
                estimate = (self.m as f64) * ((self.m as f64) / (zeros as f64)).ln();
            }
        }
        estimate as u64
    }
}

pub struct StatsGenerator {
    config: StatsConfig,
    is_physically_ordered: bool,
    total_count: u64,
    last_element: Option<Data>,
    lower_bound: Option<Data>,
    upper_bound: Option<Data>,

    reservoir: Vec<Data>,

    exact_distinct: Option<BTreeSet<OrderedData>>,
    approx_distinct: Option<HyperLogLog>,
}

impl StatsGenerator {
    /// Creates a StatsGenerator using the default configuration.
    pub fn new() -> StatsGenerator {
        Self::with_config(StatsConfig::default())
    }

    /// Creates a StatsGenerator with a custom configuration.
    pub fn with_config(config: StatsConfig) -> StatsGenerator {
        Self {
            is_physically_ordered: true,
            total_count: 0,
            last_element: None,
            lower_bound: None,
            upper_bound: None,
            reservoir: Vec::with_capacity(config.reservoir_capacity),
            exact_distinct: Some(BTreeSet::new()),
            approx_distinct: None,
            config,
        }
    }

    pub fn update(&mut self, element: Data) {
        self.total_count += 1;

        match &self.lower_bound {
            Some(bound) if element < *bound => self.lower_bound = Some(element.clone()),
            None => self.lower_bound = Some(element.clone()),
            _ => {}
        }
        match &self.upper_bound {
            Some(bound) if element > *bound => self.upper_bound = Some(element.clone()),
            None => self.upper_bound = Some(element.clone()),
            _ => {}
        }

        if let Some(last) = &self.last_element {
            if *last > element {
                self.is_physically_ordered = false;
            }
        }

        // Reservoir Sampling using Config
        if self.reservoir.len() < self.config.reservoir_capacity {
            self.reservoir.push(element.clone());
        } else {
            let mut rng = rand::thread_rng();
            let j = rng.gen_range(0..self.total_count) as usize;
            if j < self.config.reservoir_capacity {
                self.reservoir[j] = element.clone();
            }
        }

        // Cardinality Tracking using Config
        let ordered_element = OrderedData(element.clone());

        if let Some(exact_set) = &mut self.exact_distinct {
            exact_set.insert(ordered_element.clone());

            if exact_set.len() > self.config.exact_limit {
                let mut hll = HyperLogLog::new(self.config.hll_p);
                for item in exact_set.iter() {
                    hll.add(item);
                }
                self.approx_distinct = Some(hll);
                self.exact_distinct = None;
            }
        } else if let Some(hll) = &mut self.approx_distinct {
            hll.add(&ordered_element);
        }

        self.last_element = Some(element);
    }

    pub fn build(mut self) -> Vec<ColumnStat> {
        let mut stats = Vec::new();

        if self.total_count == 0 {
            return stats;
        }

        if self.is_physically_ordered {
            stats.push(ColumnStat::IsPhysicallyOrdered);
        }

        let is_string_type = matches!(self.last_element.as_ref().unwrap(), Data::String(_));
        if !is_string_type {
            stats.push(ColumnStat::RangeStat(Range {
                lower_bound: self.lower_bound.unwrap(),
                upper_bound: self.upper_bound.unwrap(),
            }));
        }

        let unique_count = match &self.exact_distinct {
            Some(exact_set) => exact_set.len() as u64,
            None => self.approx_distinct.as_ref().unwrap().count(),
        };

        stats.push(ColumnStat::CardinalityStat(CardinalityData(unique_count)));
        stats.push(ColumnStat::DensityStat(Density(
            1.0f32.min(unique_count as f32 / self.total_count as f32),
        )));

        // --- Conditional Histogram Logic ---
        // Only build histograms for strings if the cardinality is sufficiently low
        let should_build_histogram = if is_string_type {
            unique_count <= self.config.max_string_unique_for_histogram
        } else {
            true // Always build for numerics
        };

        if should_build_histogram {
            self.reservoir
                .sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

            let num_buckets = self.config.max_histogram_buckets.min(self.reservoir.len());
            let chunk_size = (self.reservoir.len() as f64 / num_buckets as f64).ceil() as usize;
            let mut frequency_points = Vec::new();

            for chunk in self.reservoir.chunks(chunk_size) {
                let sample_ratio = self.total_count as f64 / self.reservoir.len() as f64;
                let estimated_freq = (chunk.len() as f64 * sample_ratio) as u64;

                frequency_points.push((
                    Range {
                        lower_bound: chunk.first().unwrap().clone(),
                        upper_bound: chunk.last().unwrap().clone(),
                    },
                    Frequency(estimated_freq),
                ));
            }

            stats.push(ColumnStat::HistogramStat(HistogramData {
                frequency_points,
            }));
        }

        stats
    }
}
