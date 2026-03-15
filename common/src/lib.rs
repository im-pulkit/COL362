use serde::{Deserialize, Serialize};

pub mod query;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Data {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int32(l0), Self::Int32(r0)) => l0.partial_cmp(r0),
            (Self::Int64(l0), Self::Int64(r0)) => l0.partial_cmp(r0),
            (Self::Float32(l0), Self::Float32(r0)) => l0.partial_cmp(r0),
            (Self::Float64(l0), Self::Float64(r0)) => l0.partial_cmp(r0),
            (Self::String(l0), Self::String(r0)) => l0.partial_cmp(r0),
            _ => None,
        }
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int32(l0), Self::Int32(r0)) => l0 == r0,
            (Self::Int64(l0), Self::Int64(r0)) => l0 == r0,
            (Self::Float32(l0), Self::Float32(r0)) => l0 == r0,
            (Self::Float64(l0), Self::Float64(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            _ => false,
        }
    }
}
