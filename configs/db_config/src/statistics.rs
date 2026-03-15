use common::Data;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Range {
    pub lower_bound: Data,
    pub upper_bound: Data,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Frequency(pub u64);

#[derive(Debug, Deserialize, Serialize)]
pub struct Density(pub f32);

#[derive(Debug, Deserialize, Serialize)]
pub struct HistogramData {
    pub frequency_points: Vec<(Range, Frequency)>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CardinalityData(pub u64);

#[derive(Debug, Deserialize, Serialize)]
pub enum ColumnStat {
    /// Values are in ascending order when reading rows sequentially from disk.
    IsPhysicallyOrdered,
    RangeStat(Range),
    HistogramStat(HistogramData),
    CardinalityStat(CardinalityData),
    /// Represents number of unique values/ total number of value
    DensityStat(Density),
}
