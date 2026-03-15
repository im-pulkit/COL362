use common::DataType;
use serde::{Deserialize, Serialize};

use crate::statistics::ColumnStat;

#[derive(Deserialize, Serialize, Debug)]
pub struct ColumnSpec {
    pub column_name: String,
    pub data_type: DataType,
    pub stats: Option<Vec<ColumnStat>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TableSpec {
    pub name: String,
    pub file_id: String,
    pub column_specs: Vec<ColumnSpec>,
}
