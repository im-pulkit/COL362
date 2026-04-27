#![allow(dead_code, unused_variables, unused_mut, unused_assignments, unused_imports)]

use anyhow::{Context, Result};
use clap::Parser;
use common::{
    Data, DataType,
    query::{
        ComparisionOperator, ComparisionValue, CrossData, FilterData,
        ProjectData, Query, QueryOp, ScanData, SortData, SortSpec,
    },
};
use db_config::DbContext;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use crate::{cli::CliOptions, io_setup::{setup_disk_io, setup_monitor_io}};

mod cli;
mod io_setup;

type Schema = Vec<(String, DataType)>;
type Row    = Vec<Data>;

// How much RAM we allow for in-memory Sort/Cross before spilling to disk.
// Keep well under 64MB to leave room for code, stack, buffers.
const MEMORY_BUDGET_BYTES: usize = 12 * 1024 * 1024; // 12 MB
const SORT_RUN_BYTES: usize      = 12 * 1024 * 1024; // 12MB — Sort run size

// ── AnonAllocator ─────────────────────────────────────────────────────────────

struct AnonAllocator { next: u64 }

impl AnonAllocator {
    fn new(start: u64) -> Self { Self { next: start } }
    // fn alloc(&mut self, n: u64) -> u64 { let s = self.next; self.next += n; s }
}

// ── BlockWriter ───────────────────────────────────────────────────────────────

struct BlockWriter {
    block_size:  usize,
    usable:      usize,
    buf:         Vec<u8>,
    offset:      usize,
    row_count:   u16,
    start_block: u64,
    num_blocks:  u64,
    pending:     Vec<Vec<u8>>, // NEW: Buffer for sequential flushing
}

impl BlockWriter {
    fn new(block_size: u64, start: u64) -> Self {
        let bs = block_size as usize;
        Self {
            block_size: bs, usable: bs - 2, buf: vec![0u8; bs],
            offset: 0, row_count: 0, start_block: start, num_blocks: 0,
            pending: Vec::with_capacity(32),
        }
    }

    fn flush_current(&mut self) {
        if self.row_count == 0 { return; }
        let cnt = self.row_count.to_le_bytes();
        self.buf[self.block_size - 2] = cnt[0];
        self.buf[self.block_size - 1] = cnt[1];
        self.pending.push(self.buf.clone());
        self.buf.fill(0);
        self.offset = 0;
        self.row_count = 0;
    }

    fn flush_pending(&mut self, disk_out: &mut impl Write) -> Result<()> {
        if self.pending.is_empty() { return Ok(()); }
        // Write all pending blocks in a single multi-block put to avoid
        // per-block command overhead and ensure contiguous writes are atomic
        let count = self.pending.len() as u64;
        let id = self.start_block + self.num_blocks;
        disk_out.write_all(format!("put block {} {}\n", id, count).as_bytes())?;
        for p_block in &self.pending {
            disk_out.write_all(p_block)?;
        }
        disk_out.flush()?;
        self.num_blocks += count;
        self.pending.clear();
        Ok(())
    }

    fn push(&mut self, row: &Row, disk_out: &mut impl Write) -> Result<()> {
        let rb = serialize_row_bytes(row);
        self.push_bytes(&rb, disk_out)
    }

    fn push_bytes(&mut self, rb: &[u8], disk_out: &mut impl Write) -> Result<()> {
        if self.offset + rb.len() > self.usable {
            self.flush_current();
            // Flush to disk when we have 32 contiguous blocks ready
            if self.pending.len() >= 32 {
                self.flush_pending(disk_out)?;
            }
        }
        self.buf[self.offset..self.offset + rb.len()].copy_from_slice(rb);
        self.offset += rb.len();
        self.row_count += 1;
        Ok(())
    }

    fn finish(mut self, disk_out: &mut impl Write) -> Result<(u64, u64)> {
        self.flush_current();
        self.flush_pending(disk_out)?;
        Ok((self.start_block, self.num_blocks))
    }
}

// ── Disk helpers ──────────────────────────────────────────────────────────────

fn disk_cmd_u64(o: &mut impl Write, i: &mut impl BufRead, cmd: &str) -> Result<u64> {
    o.write_all(cmd.as_bytes())?; o.flush()?;
    let mut line = String::new(); i.read_line(&mut line)?;
    Ok(line.trim().parse()?)
}

fn disk_get_block_size(o: &mut impl Write, i: &mut impl BufRead) -> Result<u64> {
    disk_cmd_u64(o, i, "get block-size\n")
}

fn disk_get_file_start(o: &mut impl Write, i: &mut impl BufRead, fid: &str) -> Result<u64> {
    disk_cmd_u64(o, i, &format!("get file start-block {}\n", fid))
}

fn disk_get_file_num_blocks(o: &mut impl Write, i: &mut impl BufRead, fid: &str) -> Result<u64> {
    disk_cmd_u64(o, i, &format!("get file num-blocks {}\n", fid))
}

fn disk_read_block(
    o: &mut impl Write, i: &mut impl Read, block_id: u64, block_size: u64,
) -> Result<Vec<u8>> {
    o.write_all(format!("get block {} 1\n", block_id).as_bytes())?;
    o.flush()?;
    let mut buf = vec![0u8; block_size as usize];
    i.read_exact(&mut buf)?;
    Ok(buf)
}

fn disk_read_blocks(
    o: &mut impl Write, i: &mut impl Read, start_block: u64, num_blocks: u64, block_size: u64,
) -> Result<Vec<u8>> {
    o.write_all(format!("get block {} {}\n", start_block, num_blocks).as_bytes())?;
    o.flush()?;
    let mut buf = vec![0u8; (num_blocks as usize) * (block_size as usize)];
    i.read_exact(&mut buf)?;
    Ok(buf)
}

// ── Row parsing ───────────────────────────────────────────────────────────────

fn parse_row(buf: &[u8], mut pos: usize, col_types: &[DataType]) -> (Row, usize) {
    let mut row = Vec::with_capacity(col_types.len());
    for t in col_types {
        match t {
            DataType::Int32   => { let b:[u8;4]=buf[pos..pos+4].try_into().unwrap(); row.push(Data::Int32(i32::from_le_bytes(b)));   pos+=4; }
            DataType::Int64   => { let b:[u8;8]=buf[pos..pos+8].try_into().unwrap(); row.push(Data::Int64(i64::from_le_bytes(b)));   pos+=8; }
            DataType::Float32 => { let b:[u8;4]=buf[pos..pos+4].try_into().unwrap(); row.push(Data::Float32(f32::from_le_bytes(b))); pos+=4; }
            DataType::Float64 => { let b:[u8;8]=buf[pos..pos+8].try_into().unwrap(); row.push(Data::Float64(f64::from_le_bytes(b))); pos+=8; }
            DataType::String  => {
                let s=pos; while buf[pos]!=0x00{pos+=1;}
                row.push(Data::String(String::from_utf8_lossy(&buf[s..pos]).into_owned()));
                pos+=1;
            }
        }
    }
    (row, pos)
}

fn parse_block(block: &[u8], col_types: &[DataType]) -> Vec<Row> {
    let n=block.len();
    let count=u16::from_le_bytes([block[n-2],block[n-1]]) as usize;
    let mut rows=Vec::with_capacity(count);
    let mut offset=0;
    for _ in 0..count { let (row,next)=parse_row(block,offset,col_types); rows.push(row); offset=next; }
    rows
}

// ── Row serialization ─────────────────────────────────────────────────────────

fn rust_row_size(row: &Row) -> usize {
    24 + row.len() * 32 + row.iter().map(|d| match d {
        Data::String(s) => s.len(),
        _ => 0,
    }).sum::<usize>()
}

fn serialize_row_bytes(row: &Row) -> Vec<u8> {
    let mut b = Vec::new();
    for v in row {
        match v {
            Data::Int32(x)   => b.extend_from_slice(&x.to_le_bytes()),
            Data::Int64(x)   => b.extend_from_slice(&x.to_le_bytes()),
            Data::Float32(x) => b.extend_from_slice(&x.to_le_bytes()),
            Data::Float64(x) => b.extend_from_slice(&x.to_le_bytes()),
            Data::String(s)  => { b.extend_from_slice(s.as_bytes()); b.push(0); }
        }
    }
    b
}

fn approx_row_size(row: &Row) -> usize {
    // Vec<Data> overhead + per-element overhead
    let base = 24; // Vec struct
    let values: usize = row.iter().map(|v| match v {
        Data::Int32(_)   => 24,
        Data::Int64(_)   => 24,
        Data::Float32(_) => 24,
        Data::Float64(_) => 24,
        Data::String(s)  => 56 + s.len(), // enum(8) + String(24) + heap + padding
    }).sum();
    base + values + row.len() * 8 // extra padding per element
}

// ── Output formatting ─────────────────────────────────────────────────────────

fn format_value(val: &Data) -> String {
    match val {
        Data::Int32(v)   => v.to_string(),
        Data::Int64(v)   => v.to_string(),
        Data::Float32(v) => format_float(*v as f64),
        Data::Float64(v) => format_float(*v),
        Data::String(s)  => s.clone(),
    }
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 { format!("{:.1}", v) }
    else { format!("{}", v) }
}

fn emit_row_to_monitor(row: &Row, out: &mut impl Write) -> Result<()> {
    for val in row { out.write_all(format_value(val).as_bytes())?; out.write_all(b"|")?; }
    out.write_all(b"\n")?;
    Ok(())
}

// ── Schema helpers ────────────────────────────────────────────────────────────

fn col_idx(schema: &Schema, name: &str) -> usize {
    schema.iter().position(|(n,_)| n==name)
        .unwrap_or_else(|| panic!("Column '{}' not found",name))
}

fn project_schema(schema: &Schema, p: &ProjectData) -> Schema {
    let mut res = Vec::with_capacity(p.column_name_map.len());
    for (from, to) in &p.column_name_map {
        let i = col_idx(schema, from);
        res.push((to.clone(), schema[i].1.clone()));
    }
    res
}

fn project_row(row: &Row, schema: &Schema, p: &ProjectData) -> Row {
    let mut out = Vec::with_capacity(p.column_name_map.len());
    let mut indices = Vec::with_capacity(p.column_name_map.len());
    for (from, _) in &p.column_name_map { indices.push(col_idx(schema, from)); }
    for i in indices { out.push(row[i].clone()); }
    out
}

fn apply_project_chain(row: Row, base: &Schema, projects: &[&ProjectData]) -> Row {
    if projects.is_empty() { return row; }
    let mut r=row; let mut s=base.clone();
    for p in projects { let nr=project_row(&r,&s,p); s=project_schema(&s,p); r=nr; }
    r
}

// ── Statistics Access Helpers ─────────────────────────────────────────────────

/// Get the row cardinality of a table from its CardinalityStat.
fn stat_cardinality(table_name: &str, ctx: &DbContext) -> Option<u64> {
    use db_config::statistics::ColumnStat;
    let table = ctx.get_table_specs().iter().find(|t| t.name == table_name)?;
    // Cardinality is the same for every column — pick the first one with stats
    for col in &table.column_specs {
        if let Some(stats) = &col.stats {
            for s in stats {
                if let ColumnStat::CardinalityStat(c) = s {
                    return Some(c.0);
                }
            }
        }
    }
    None
}

/// Get density (unique_values / total_rows) for a column. 1.0 = unique key.
fn stat_density(table_name: &str, col_name: &str, ctx: &DbContext) -> Option<f64> {
    use db_config::statistics::ColumnStat;
    let table = ctx.get_table_specs().iter().find(|t| t.name == table_name)?;
    let col   = table.column_specs.iter().find(|c| c.column_name == col_name)?;
    let stats = col.stats.as_ref()?;
    for s in stats {
        if let ColumnStat::DensityStat(d) = s {
            return Some(d.0 as f64);
        }
    }
    None
}

/// Number of distinct values in a column = density * cardinality.
fn stat_distinct_values(table_name: &str, col_name: &str, ctx: &DbContext) -> Option<u64> {
    let card    = stat_cardinality(table_name, ctx)?;
    let density = stat_density(table_name, col_name, ctx)?;
    Some(((card as f64) * density).max(1.0) as u64)
}

/// Get the [lower, upper] range for a column, if known.
fn stat_range(table_name: &str, col_name: &str, ctx: &DbContext) -> Option<(Data, Data)> {
    use db_config::statistics::ColumnStat;
    let table = ctx.get_table_specs().iter().find(|t| t.name == table_name)?;
    let col   = table.column_specs.iter().find(|c| c.column_name == col_name)?;
    let stats = col.stats.as_ref()?;
    for s in stats {
        if let ColumnStat::RangeStat(r) = s {
            return Some((r.lower_bound.clone(), r.upper_bound.clone()));
        }
    }
    None
}

/// Check if a column is a unique key (density = 1.0).
fn stat_is_unique(table_name: &str, col_name: &str, ctx: &DbContext) -> bool {
    stat_density(table_name, col_name, ctx)
        .map_or(false, |d| (d - 1.0).abs() < 0.001)
}

/// Estimate the number of blocks for a table (for I/O cost calculations).
fn stat_blocks(table_name: &str, ctx: &DbContext, disk_out: &mut impl Write,
               disk_in: &mut (impl BufRead + Read)) -> Option<u64> {
    let table = ctx.get_table_specs().iter().find(|t| t.name == table_name)?;
    disk_get_file_num_blocks(disk_out, disk_in, &table.file_id).ok()
}

/// Estimate blocks for a table using cardinality (no disk access required).
/// Assumes ~80 bytes per row for tpch tables → ~50 rows per 4KB block.
fn stat_blocks_estimate(table_name: &str, ctx: &DbContext) -> u64 {
    stat_cardinality(table_name, ctx)
        .map(|c| (c / 50).max(1))
        .unwrap_or(1)
}

// ── Cost Model ────────────────────────────────────────────────────────────────
// Cost is in "block I/O units". 1 unit = reading one block.
// Writes cost more than reads due to seek patterns.

const COST_READ:  f64 = 1.0;
const COST_WRITE: f64 = 2.0;
/// Memory budget in blocks (12MB / 4KB = 3072 blocks).
const MEMORY_BLOCKS: u64 = 3072;

/// Number of blocks to read in a single batched I/O to reduce rotational latency.
const READ_AHEAD_BLOCKS: u64 = 32;

/// Prefetch blocks per run during k-way merge — keep very small to limit memory.
const MERGE_PREFETCH_BLOCKS: u64 = 16;

/// Cost of sequentially scanning a table.
fn cost_seq_scan(blocks: u64) -> f64 {
    blocks as f64 * COST_READ
}

/// Cost of nested loop join: O(N × M) row comparisons + scan both tables.
/// Reads inner table once per outer row → catastrophic for large tables.
fn cost_nested_loop(left_blocks: u64, right_blocks: u64, left_rows: u64) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    l * COST_READ + (left_rows as f64) * r * COST_READ
}

/// Cost of block nested loop: reads inner table once per BLOCK of outer.
/// Much better than NLJ when tables are larger than memory.
fn cost_block_nl(left_blocks: u64, right_blocks: u64) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    l * COST_READ + l * r * COST_READ
}

/// Cost of in-memory hash join (build side fits in RAM).
/// Read both sides once: O(N + M). Best for small × large.
fn cost_hash_join_simple(left_blocks: u64, right_blocks: u64) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    (l + r) * COST_READ
}

/// Cost of grace hash join: partition both sides to disk, then join partitions.
/// 3 × (R + S) — read both, write both, read both again.
fn cost_hash_join_grace(left_blocks: u64, right_blocks: u64) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    (l + r) * COST_READ + (l + r) * COST_WRITE + (l + r) * COST_READ
}

/// Cost of sort-merge join when BOTH sides are already physically ordered.
/// Just streams both — best possible.
fn cost_sort_merge_ordered(left_blocks: u64, right_blocks: u64) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    (l + r) * COST_READ
}

/// Cost of sort-merge join when one or both sides need sorting first.
/// Sort cost = 2 × N × log(N/M) where M is memory budget.
fn cost_sort_merge_with_sort(
    left_blocks: u64, right_blocks: u64,
    left_needs_sort: bool, right_needs_sort: bool,
    memory_blocks: u64,
) -> f64 {
    let l = left_blocks as f64;
    let r = right_blocks as f64;
    let mut total = (l + r) * COST_READ; // final merge phase reads both
    if left_needs_sort  { total += cost_external_sort(left_blocks,  memory_blocks); }
    if right_needs_sort { total += cost_external_sort(right_blocks, memory_blocks); }
    total
}

/// Cost of external merge sort: 2 × blocks × num_passes.
fn cost_external_sort(blocks: u64, memory_blocks: u64) -> f64 {
    if blocks <= memory_blocks {
        // Fits in memory — single pass
        return blocks as f64 * COST_READ;
    }
    let b = blocks as f64;
    let m = (memory_blocks.max(2)) as f64;
    let passes = (b.log2() / m.log2()).ceil().max(1.0);
    2.0 * b * passes
}

/// Estimate join output cardinality given inputs and join key densities.
/// For equi-join R.A = S.B with densities dA, dB:
///   |R ⋈ S| = (|R| × |S|) / max(distinct(R.A), distinct(S.B))
fn estimate_join_cardinality(
    left_card:    u64,
    right_card:   u64,
    left_distinct:  Option<u64>,
    right_distinct: Option<u64>,
) -> u64 {
    let max_distinct = left_distinct.unwrap_or(left_card)
        .max(right_distinct.unwrap_or(right_card))
        .max(1);
    ((left_card as f64) * (right_card as f64) / (max_distinct as f64)) as u64
}

/// Estimate selectivity of a single predicate.
/// EQ on column C with N distinct values → selectivity = 1/N
/// Range predicates → use range stats
/// Other → 0.1 default

fn coerce_to_f64_lenient(v: &Data) -> Option<f64> {
    match v {
        Data::Int32(x)   => Some(*x as f64),
        Data::Int64(x)   => Some(*x as f64),
        Data::Float32(x) => Some(*x as f64),
        Data::Float64(x) => Some(*x),
        Data::String(s)  => s.replace("-", "").parse::<f64>().ok(),
    }
}

fn estimate_predicate_selectivity(
    pred:        &common::query::Predicate,
    table_name:  &str,
    col_name:    &str,
    ctx:         &DbContext,
) -> f64 {
    use ComparisionOperator as Op;
    use ComparisionValue as V;

    match (&pred.operator, &pred.value) {
        (Op::EQ, V::Column(_)) => 1.0,
        (Op::EQ, _) => {
            stat_distinct_values(table_name, col_name, ctx)
                .map(|n| 1.0 / (n as f64))
                .unwrap_or(0.1)
        }
        (Op::NE, _) => 0.9,
        (Op::LT | Op::LTE | Op::GT | Op::GTE, val) => {
            // Actually use the range stats!
            if let Some((min_data, max_data)) = stat_range(table_name, col_name, ctx) {
                let min = coerce_to_f64_lenient(&min_data);
                let max = coerce_to_f64_lenient(&max_data);
                let target = match val {
                    V::I32(x) => Some(*x as f64),
                    V::I64(x) => Some(*x as f64),
                    V::F32(x) => Some(*x as f64),
                    V::F64(x) => Some(*x),
                    V::String(s) => s.replace("-", "").parse::<f64>().ok(),
                    _ => None,
                };

                if let (Some(mn), Some(mx), Some(t)) = (min, max, target) {
                    if mx > mn {
                        let ratio = (t - mn) / (mx - mn);
                        let sel = match &pred.operator {
                            Op::LT | Op::LTE => ratio,
                            Op::GT | Op::GTE => 1.0 - ratio,
                            _ => 0.33,
                        };
                        return sel.clamp(0.01, 1.0); // Keep it sane between 1% and 100%
                    }
                }
            }
            0.33 // Fallback if no stats exist
        }
    }
}

// ── Cardinality Estimator ─────────────────────────────────────────────────────
// Estimates the size of intermediate results given a set of joined tables
// and applied predicates.

use std::collections::HashMap;

/// A single relation's contribution to a join subset
#[derive(Clone)]
struct RelationInfo {
    table_name:  String,
    cardinality: u64,
    blocks:      u64,
    schema:      Schema,
}

impl RelationInfo {
    fn from_op(op: &QueryOp, ctx: &DbContext) -> Option<Self> {
        let table_name = get_scan_table_name(op)?;
        let cardinality = stat_cardinality(&table_name, ctx).unwrap_or(1);
        let blocks      = stat_blocks_estimate(&table_name, ctx);
        let schema      = schema_of(op, ctx);
        Some(Self { table_name, cardinality, blocks, schema })
    }
}

/// Estimate the cardinality of a subset of joined relations after applying
/// all relevant predicates. Used by DP to score plans.
fn estimate_subset_cardinality(
    relations:  &[&RelationInfo],
    predicates: &[common::query::Predicate],
    ctx:        &DbContext,
) -> u64 {
    if relations.is_empty() { return 0; }

    // Start with cartesian product
    let mut total: f64 = relations.iter()
        .map(|r| r.cardinality as f64).product();

    // Combined schema for predicate matching
    let combined: Vec<&str> = relations.iter()
        .flat_map(|r| r.schema.iter().map(|(n,_)| n.as_str()))
        .collect();

    // Apply each predicate's selectivity
    for pred in predicates {
        let lhs_in = combined.iter().any(|n| n == &pred.column_name);
        let rhs_in = match &pred.value {
            ComparisionValue::Column(c) => combined.iter().any(|n| n == c),
            _ => true,
        };
        if !lhs_in || !rhs_in { continue; }

        let selectivity = match &pred.value {
            ComparisionValue::Column(other) => {
                // Equi-join: selectivity = 1 / max(distinct(A), distinct(B))
                let lhs_distinct = relations.iter()
                    .find(|r| r.schema.iter().any(|(n,_)| n == &pred.column_name))
                    .and_then(|r| stat_distinct_values(&r.table_name, &pred.column_name, ctx));
                let rhs_distinct = relations.iter()
                    .find(|r| r.schema.iter().any(|(n,_)| n == other))
                    .and_then(|r| stat_distinct_values(&r.table_name, other, ctx));
                let max_d = lhs_distinct.unwrap_or(1000).max(rhs_distinct.unwrap_or(1000)).max(1);
                1.0 / (max_d as f64)
            }
            _ => {
                // Literal predicate
                let table = relations.iter()
                    .find(|r| r.schema.iter().any(|(n,_)| n == &pred.column_name));
                if let Some(rel) = table {
                    estimate_predicate_selectivity(pred, &rel.table_name, &pred.column_name, ctx)
                } else {
                    0.1
                }
            }
        };

        total *= selectivity;
    }

    total.max(1.0) as u64
}

/// Estimate the I/O cost of executing a join with a given algorithm

fn estimate_join_cost(
    left_blocks:  u64,
    right_blocks: u64,
    left_card:    u64,
    right_card:   u64,
    left_ordered_on_join_key:  bool,
    right_ordered_on_join_key: bool,
    has_equi_pred: bool,
) -> (f64, &'static str) {
    if !has_equi_pred {
        // Fallback to cross nested loop if no equi-join exists
        return (cost_nested_loop(left_blocks, right_blocks, left_card), "nested_loop");
    }

    // Mirror the exact logic inside `exec_cross` so the DP costs what is ACTUALLY executed!
    
    // 1. Both ordered? Exec engine chooses Sort-Merge
    if left_ordered_on_join_key && right_ordered_on_join_key {
        return (cost_sort_merge_ordered(left_blocks, right_blocks), "sort_merge");
    }

    // 2. Smaller side fits in RAM? Exec engine chooses Simple Hash
    let min_card = left_card.min(right_card);
    if min_card <= 100000 {
        return (cost_hash_join_simple(left_blocks, right_blocks), "hash_join_simple");
    }

    // 3. Otherwise, Exec engine falls back to Grace Hash
    (cost_hash_join_grace(left_blocks, right_blocks), "hash_join_grace")
}

/// Find equi-join predicates between two sets of schemas
fn find_equi_predicates<'a>(
    left_schemas:  &[&Schema],
    right_schemas: &[&Schema],
    predicates:    &'a [common::query::Predicate],
) -> Vec<&'a common::query::Predicate> {
    predicates.iter().filter(|p| {
        if p.operator != ComparisionOperator::EQ { return false; }
        let ComparisionValue::Column(other) = &p.value else { return false; };
        let lhs_left  = left_schemas.iter().any(|s| s.iter().any(|(n,_)| n == &p.column_name));
        let rhs_right = right_schemas.iter().any(|s| s.iter().any(|(n,_)| n == other.as_str()));
        let lhs_right = right_schemas.iter().any(|s| s.iter().any(|(n,_)| n == &p.column_name));
        let rhs_left  = left_schemas.iter().any(|s| s.iter().any(|(n,_)| n == other.as_str()));
        (lhs_left && rhs_right) || (lhs_right && rhs_left)
    }).collect()
}

// ── DP Join Order Optimizer ───────────────────────────────────────────────────

/// Entry in the DP table — best plan for a subset of relations
#[derive(Clone)]
struct DPEntry {
    plan:        QueryOp,
    cost:        f64,
    cardinality: u64,
    schema:      Schema,
}

/// Bitmask-based DP join optimizer.
/// Considers all left-deep plans, picks minimum cost.
fn dp_optimize(
    relations:  Vec<QueryOp>,
    predicates: Vec<common::query::Predicate>,
    ctx:        &DbContext,
) -> QueryOp {
    let n = relations.len();
    if n == 1 {
        let r = relations.into_iter().next().unwrap();
        return if predicates.is_empty() { r }
        else { QueryOp::Filter(FilterData { predicates, underlying: Box::new(r) }) };
    }

    // Detect self-joins (renamed columns) — DP doesn't handle these correctly
    // Fall back to greedy in that case
    // let has_renamed = relations.iter().any(|op| matches!(op, QueryOp::Project(_)));
    // if has_renamed {
    //     return build_incremental_join(relations, predicates, ctx);
    // }

    // Build relation info for each base table
    let rel_infos: Vec<RelationInfo> = relations.iter()
        .map(|op| RelationInfo::from_op(op, ctx).unwrap_or_else(|| {
            // Fallback for non-Scan ops (Project wrappers, etc.)
            let card = estimate_scan_cardinality(op, ctx);
            RelationInfo {
                table_name: get_scan_table_name(op).unwrap_or_else(|| "complex".into()),
                cardinality: card,
                blocks: (card / 50).max(1),
                schema: schema_of(op, ctx),
            }
        }))
        .collect();

    // dp[bitmask] = best plan for that subset
    let mut dp: HashMap<u32, DPEntry> = HashMap::new();
    // Base case: single relations with their literal predicates applied
    for i in 0..n {
        let mask = 1u32 << i;
        let rel = &rel_infos[i];

        // Apply literal predicates that reference only this table
        let local_preds: Vec<common::query::Predicate> = predicates.iter()
            .filter(|p| {
                let lhs_in = rel.schema.iter().any(|(n,_)| n == &p.column_name);
                // Check if the right-hand side is either a literal OR a column in the SAME table
                let rhs_in = match &p.value {
                    ComparisionValue::Column(c) => rel.schema.iter().any(|(n,_)| n == c),
                    _ => true, 
                };
                lhs_in && rhs_in
            })
            .cloned()
            .collect();

        // Estimate cardinality after local filters
        let mut card = rel.cardinality as f64;
        for p in &local_preds {
            card *= estimate_predicate_selectivity(p, &rel.table_name, &p.column_name, ctx);
        }
        let filtered_card = card.max(1.0) as u64;

        let plan = if local_preds.is_empty() {
            relations[i].clone()
        } else {
            QueryOp::Filter(FilterData {
                predicates: local_preds,
                underlying: Box::new(relations[i].clone()),
            })
        };

        // Cost = scanning the table
        let cost = cost_seq_scan(rel.blocks);

        dp.insert(mask, DPEntry {
            plan,
            cost,
            cardinality: filtered_card,
            schema: rel.schema.clone(),
        });
    }

    // Build up subsets of size 2..n
    for size in 2..=n {
        let subsets = enumerate_subsets(n, size);
        for mask in subsets {
            let mut best: Option<DPEntry> = None;

            // Try all ways to split: subset = (subset \ {i}) ⋈ {i}
            for i in 0..n {
                if mask & (1 << i) == 0 { continue; }
                let left_mask  = mask & !(1u32 << i);
                let right_mask = 1u32 << i;
                if left_mask == 0 { continue; }

                let left  = match dp.get(&left_mask)  { Some(e) => e, None => continue };
                let right = match dp.get(&right_mask) { Some(e) => e, None => continue };

                // Find equi-predicates between left and right
                let join_preds: Vec<&common::query::Predicate> = predicates.iter().filter(|p| {
                    let lhs_left  = left.schema.iter().any(|(n,_)| n == &p.column_name);
                    let lhs_right = right.schema.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_left  = match &p.value {
                        ComparisionValue::Column(c) => left.schema.iter().any(|(n,_)| n == c),
                        _ => false,
                    };
                    let rhs_right = match &p.value {
                        ComparisionValue::Column(c) => right.schema.iter().any(|(n,_)| n == c),
                        _ => false,
                    };
                    (lhs_left && rhs_right) || (lhs_right && rhs_left)
                }).collect();

                // Skip cartesian products unless this is the last option
                let has_equi = join_preds.iter().any(|p| {
                    p.operator == ComparisionOperator::EQ
                    && matches!(&p.value, ComparisionValue::Column(_))
                });
                if !has_equi && size < n {
                    // Avoid cartesian for intermediate subsets
                    continue;
                }

                // Gather the actual relations present in this specific joined subset
                let mut subset_rels = Vec::new();
                for j in 0..n {
                    if (mask & (1u32 << j)) != 0 {
                        subset_rels.push(&rel_infos[j]);
                    }
                }

                // Utilize the orphaned, highly-accurate cardinality estimator!
                let join_card = estimate_subset_cardinality(&subset_rels, &predicates, ctx);

                // Estimate cost of the join
                let left_blocks  = (left.cardinality  / 50).max(1);
                let right_blocks = (right.cardinality / 50).max(1);

                // Check if join keys are physically ordered (for sort-merge)
                let (left_ordered, right_ordered) = if let Some(equi_pred) = join_preds.iter().find(|p| {
                    p.operator == ComparisionOperator::EQ &&
                    matches!(&p.value, ComparisionValue::Column(_))
                }) {
                    let other = if let ComparisionValue::Column(c) = &equi_pred.value { c } else { "" };
                    let left_col = if left.schema.iter().any(|(n,_)| n == &equi_pred.column_name) {
                        &equi_pred.column_name
                    } else { other };
                    let right_col = if right.schema.iter().any(|(n,_)| n == other) {
                        other
                    } else { &equi_pred.column_name };
                    let lt = single_table_name(&left.plan);
                    let rt = single_table_name(&right.plan);
                    let lo = lt.as_ref().map_or(false, |t| is_physically_ordered(left_col, t, ctx));
                    let ro = rt.as_ref().map_or(false, |t| is_physically_ordered(right_col, t, ctx));
                    (lo, ro)
                } else { (false, false) };

                let (join_cost, _algo) = estimate_join_cost(
                    left_blocks, right_blocks,
                    left.cardinality, right.cardinality,
                    left_ordered, right_ordered, has_equi,
                );

                let total_cost = left.cost + right.cost + join_cost;

                if best.as_ref().map_or(true, |b| total_cost < b.cost) {
                    // Build the actual plan
                    let cross = QueryOp::Cross(CrossData {
                        left:  Box::new(left.plan.clone()),
                        right: Box::new(right.plan.clone()),
                    });
                    let plan = if join_preds.is_empty() { cross }
                    else { QueryOp::Filter(FilterData {
                        predicates: join_preds.iter().map(|p| (*p).clone()).collect(),
                        underlying: Box::new(cross),
                    })};
                    let combined_schema: Schema = left.schema.iter()
                        .chain(right.schema.iter()).cloned().collect();
                    best = Some(DPEntry {
                        plan,
                        cost: total_cost,
                        cardinality: join_card,
                        schema: combined_schema,
                    });
                }
            }

            if let Some(entry) = best {
                dp.insert(mask, entry);
            }
        }
    }

    let full_mask = (1u32 << n) - 1;

    // Get the final plan, fall back to incremental join if DP failed
    let final_entry = match dp.remove(&full_mask) {
        Some(e) => e,
        None => return build_incremental_join(relations, predicates, ctx),
    };

    // Apply any remaining predicates that weren't handled
    let used_preds: Vec<bool> = vec![false; predicates.len()];
    // Simplification: just apply any leftover predicates on top
    let leftover: Vec<common::query::Predicate> = predicates.into_iter()
        .filter(|p| {
            let in_schema = final_entry.schema.iter().any(|(n,_)| n == &p.column_name);
            let rhs_in = match &p.value {
                ComparisionValue::Column(c) => final_entry.schema.iter().any(|(n,_)| n == c),
                _ => true,
            };
            in_schema && rhs_in
        })
        .collect();

    if leftover.is_empty() {
        final_entry.plan
    } else {
        QueryOp::Filter(FilterData {
            predicates: leftover,
            underlying: Box::new(final_entry.plan),
        })
    }
}

/// Enumerate all subsets of {0..n-1} with given popcount.
fn enumerate_subsets(n: usize, size: usize) -> Vec<u32> {
    let mut result = Vec::new();
    let max = 1u32 << n;
    for mask in 1..max {
        if (mask.count_ones() as usize) == size {
            result.push(mask);
        }
    }
    result
}

/// Get the table name if this op is a single Scan or Filter(Scan) etc.
fn single_table_name(op: &QueryOp) -> Option<String> {
    match op {
        QueryOp::Scan(s) => Some(s.table_id.clone()),
        QueryOp::Filter(f) => single_table_name(&f.underlying),
        QueryOp::Project(p) => single_table_name(&p.underlying),
        _ => None,
    }
}

// ── Filter evaluation ─────────────────────────────────────────────────────────

fn coerce_to_f64(v: &Data) -> Option<f64> {
    match v {
        Data::Int32(x)   => Some(*x as f64),
        Data::Int64(x)   => Some(*x as f64),
        Data::Float32(x) => Some(*x as f64),
        Data::Float64(x) => Some(*x),
        _                => None,
    }
}

fn eval_predicate(row: &Row, schema: &Schema, pred: &common::query::Predicate) -> bool {
    let left_idx = col_idx(schema, &pred.column_name);
    let left = &row[left_idx];
    let right = match &pred.value {
        ComparisionValue::Column(c) => {
            let right_idx = col_idx(schema, c);
            row[right_idx].clone()
        }
        ComparisionValue::I32(v)    => Data::Int32(*v),
        ComparisionValue::I64(v)    => Data::Int64(*v),
        ComparisionValue::F32(v)    => Data::Float32(*v),
        ComparisionValue::F64(v)    => Data::Float64(*v),
        ComparisionValue::String(s) => Data::String(s.clone()),
    };

    // Try numeric coercion first (handles cross-type comparisons like Float64 vs I32)
    if let (Some(l), Some(r)) = (coerce_to_f64(left), coerce_to_f64(&right)) {
        return match &pred.operator {
            ComparisionOperator::EQ  => l == r,
            ComparisionOperator::NE  => l != r,
            ComparisionOperator::GT  => l > r,
            ComparisionOperator::GTE => l >= r,
            ComparisionOperator::LT  => l < r,
            ComparisionOperator::LTE => l <= r,
        };
    }

    // Same-type comparison (strings, or same numeric types)
    match &pred.operator {
        ComparisionOperator::EQ  => left == &right,
        ComparisionOperator::NE  => left != &right,
        ComparisionOperator::GT  => matches!(left.partial_cmp(&right), Some(std::cmp::Ordering::Greater)),
        ComparisionOperator::GTE => matches!(left.partial_cmp(&right), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
        ComparisionOperator::LT  => matches!(left.partial_cmp(&right), Some(std::cmp::Ordering::Less)),
        ComparisionOperator::LTE => matches!(left.partial_cmp(&right), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
    }
}

fn passes_all_filters(row: &Row, schema: &Schema, filters: &[&FilterData]) -> bool {
    filters.iter().all(|f| f.predicates.iter().all(|p| eval_predicate(row,schema,p)))
}

fn passes_predicates(row: &Row, schema: &Schema, preds: &[common::query::Predicate]) -> bool {
    preds.iter().all(|p| eval_predicate(row, schema, p))
}

// ── Pipeline ──────────────────────────────────────────────────────────────────

struct Pipeline<'a> {
    scan:     &'a ScanData,
    filters:  Vec<&'a FilterData>,
    projects: Vec<&'a ProjectData>,
}

fn try_flatten(op: &QueryOp) -> Option<Pipeline<'_>> {
    let mut filters=Vec::new(); let mut projects=Vec::new();
    let scan=try_flatten_inner(op,&mut filters,&mut projects)?;
    Some(Pipeline{scan,filters,projects})
}

fn try_flatten_inner<'a>(
    op: &'a QueryOp, filters: &mut Vec<&'a FilterData>, projects: &mut Vec<&'a ProjectData>,
) -> Option<&'a ScanData> {
    match op {
        QueryOp::Scan(s)    => Some(s),
        QueryOp::Filter(f)  => { let s=try_flatten_inner(&f.underlying,filters,projects)?; filters.push(f);  Some(s) }
        QueryOp::Project(p) => { let s=try_flatten_inner(&p.underlying,filters,projects)?; projects.push(p); Some(s) }
        _                   => None,
    }
}

fn pipeline_output_schema(base: &Schema, p: &Pipeline) -> Schema {
    let mut s=base.clone(); for proj in &p.projects { s=project_schema(&s,proj); } s
}

fn exec_pipeline(
    pipeline: &Pipeline, ctx: &DbContext,
    disk_out: &mut impl Write, disk_in: &mut (impl BufRead + Read),
    block_size: u64, emit: &mut dyn FnMut(Row,&Schema)->Result<()>,
) -> Result<Schema> {
    let table = ctx.get_table_specs().iter()
        .find(|t| t.name==pipeline.scan.table_id)
        .with_context(|| format!("Table '{}' not found",pipeline.scan.table_id))?;

    let base_schema: Schema = table.column_specs.iter()
        .map(|c|(c.column_name.clone(),c.data_type.clone())).collect();
    let col_types: Vec<DataType>=base_schema.iter().map(|(_,t)|t.clone()).collect();
    let out_schema=pipeline_output_schema(&base_schema,pipeline);
    // let (pre_filters, post_filters) = split_filters_by_schema(&pipeline.filters, &base_schema);

    // Split filters: pre-projection (all cols in base_schema) vs post-projection
    let (pre_filters, post_filters): (Vec<_>, Vec<_>) = pipeline.filters.iter()
        .partition(|f| {
            f.predicates.iter().all(|p| {
                let lhs_ok = base_schema.iter().any(|(n,_)| n == &p.column_name);
                let rhs_ok = match &p.value {
                    ComparisionValue::Column(c) => base_schema.iter().any(|(n,_)| n == c),
                    _ => true,
                };
                lhs_ok && rhs_ok
            })
        });

    let start=disk_get_file_start(disk_out,disk_in,&table.file_id)?;
    let num  =disk_get_file_num_blocks(disk_out,disk_in,&table.file_id)?;

    let mut idx = 0u64;
    while idx < num {
        let n = READ_AHEAD_BLOCKS.min(num - idx);
        let data = disk_read_blocks(disk_out, disk_in, start + idx, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &col_types) {
                if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
                let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
                if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }
                emit(final_row, &out_schema)?;
            }
        }
        idx += n;
    }
    Ok(out_schema)
}

// ── Collect helper ────────────────────────────────────────────────────────────
// Collects all rows from an op into a Vec, tracking total byte size.

fn collect_rows(
    op: &QueryOp, ctx: &DbContext,
    disk_out: &mut impl Write, disk_in: &mut (impl BufRead + Read),
    block_size: u64, allocator: &mut AnonAllocator,
) -> Result<(Schema, Vec<Row>, usize)> {
    let mut rows: Vec<Row> = Vec::new();
    let mut total_bytes: usize = 0;
    let schema = execute(op, ctx, disk_out, disk_in, block_size, allocator,
        &mut |row, _| {
            total_bytes += approx_row_size(&row);
            rows.push(row);
            Ok(())
        })?;
    Ok((schema, rows, total_bytes))
}

// ── Spill Vec<Row> to anon disk ────────────────────────────────────────────────

fn spill_rows_to_anon(
    rows: &[Row], disk_out: &mut impl Write, block_size: u64, allocator: &mut AnonAllocator,
) -> Result<(u64, u64)> {
    if rows.is_empty() { return Ok((allocator.next, 0)); }
    let start = allocator.next;
    let mut writer = BlockWriter::new(block_size, start);
    for row in rows { writer.push(row, disk_out)?; }
    let (sb, nb) = writer.finish(disk_out)?;
    allocator.next = start + nb;
    Ok((sb, nb))
}

// ── Sort-Merge Join ───────────────────────────────────────────────────────────

// ── Streaming Sort-Merge Join ─────────────────────────────────────────────────
// Reads both sides one block at a time — O(1) memory regardless of table size.

fn is_physically_ordered(col_name: &str, table_name: &str, ctx: &DbContext) -> bool {
    use db_config::statistics::ColumnStat;
    let Some(table) = ctx.get_table_specs().iter().find(|t| t.name == table_name)
        else { return false; };
    let Some(col) = table.column_specs.iter().find(|c| c.column_name == col_name)
        else { return false; };
    col.stats.as_ref().map_or(false, |stats| {
        stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered))
    })
}

struct StreamingPipelineReader<'a> {
    pipeline:    Pipeline<'a>,
    base_schema: Schema,
    col_types:   Vec<DataType>,
    out_schema:  Schema,
    start_block: u64,
    num_blocks:  u64,
    cur_block:   u64,
    rows:        Vec<Row>,
    row_idx:     usize,
}

impl<'a> StreamingPipelineReader<'a> {
    fn new(
        pipeline:   Pipeline<'a>,
        ctx:        &DbContext,
        disk_out:   &mut impl Write,
        disk_in:    &mut (impl BufRead + Read),
        block_size: u64,
    ) -> Result<Self> {
        let table = ctx.get_table_specs().iter()
            .find(|t| t.name == pipeline.scan.table_id)
            .with_context(|| format!("Table '{}' not found", pipeline.scan.table_id))?;

        let base_schema: Schema = table.column_specs.iter()
            .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
        let col_types: Vec<DataType> = base_schema.iter()
            .map(|(_, t)| t.clone()).collect();
        let out_schema = pipeline_output_schema(&base_schema, &pipeline);
        let start_block = disk_get_file_start(disk_out, disk_in, &table.file_id)?;
        let num_blocks  = disk_get_file_num_blocks(disk_out, disk_in, &table.file_id)?;

        Ok(Self {
            pipeline, base_schema, col_types, out_schema,
            start_block, num_blocks, cur_block: 0,
            rows: Vec::new(), row_idx: 0,
        })
    }

    // Load the next block from disk, returns false if exhausted
    // Replace your existing load_next_block method inside StreamingPipelineReader
    fn load_next_block(
        &mut self,
        disk_out:   &mut impl Write,
        disk_in:    &mut (impl BufRead + Read),
        block_size: u64,
    ) -> Result<bool> {
        if self.cur_block >= self.num_blocks { return Ok(false); }
        
        self.rows.clear();
        let blocks_to_read = READ_AHEAD_BLOCKS.min(self.num_blocks - self.cur_block);
        let data = disk_read_blocks(disk_out, disk_in, self.start_block + self.cur_block, blocks_to_read, block_size)?;
        for b in 0..blocks_to_read as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            self.rows.extend(parse_block(block, &self.col_types));
            self.cur_block += 1;
        }
        
        self.row_idx = 0;
        Ok(true)
    }

    // Advance to next row that passes filters, loading blocks as needed
    fn advance(
        &mut self,
        disk_out:   &mut impl Write,
        disk_in:    &mut (impl BufRead + Read),
        block_size: u64,
    ) -> Result<()> {
        self.row_idx += 1;
        loop {
            while self.row_idx < self.rows.len() {
                if passes_all_filters(&self.rows[self.row_idx],
                    &self.base_schema, &self.pipeline.filters) {
                    return Ok(());
                }
                self.row_idx += 1;
            }
            // Current block exhausted, load next
            if !self.load_next_block(disk_out, disk_in, block_size)? {
                return Ok(()); // exhausted
            }
        }
    }

    // Initialize — load first valid row
    fn init(
        &mut self,
        disk_out:   &mut impl Write,
        disk_in:    &mut (impl BufRead + Read),
        block_size: u64,
    ) -> Result<()> {
        self.load_next_block(disk_out, disk_in, block_size)?;
        // Find first valid row
        if !self.rows.is_empty() &&
            !passes_all_filters(&self.rows[0], &self.base_schema, &self.pipeline.filters) {
            self.advance(disk_out, disk_in, block_size)?;
        }
        Ok(())
    }

    fn current(&self) -> Option<Row> {
        if self.row_idx >= self.rows.len() { return None; }
        let row = self.rows[self.row_idx].clone();
        Some(apply_project_chain(row, &self.base_schema, &self.pipeline.projects))
    }

    fn is_exhausted(&self) -> bool {
        self.row_idx >= self.rows.len() && self.cur_block >= self.num_blocks
    }
}

fn exec_sort_merge_join(
    cross_data:       &CrossData,
    left_join_col:    &str,
    right_join_col:   &str,
    theta_predicates: &[&common::query::Predicate],
    ctx:              &DbContext,
    disk_out:         &mut impl Write,
    disk_in:          &mut (impl BufRead + Read),
    block_size:       u64,
    emit:             &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    let left_pipeline  = try_flatten(&cross_data.left)
        .expect("sort-merge: left must be pipeline");
    let right_pipeline = try_flatten(&cross_data.right)
        .expect("sort-merge: right must be pipeline");

    let mut left  = StreamingPipelineReader::new(left_pipeline,  ctx, disk_out, disk_in, block_size)?;
    let mut right = StreamingPipelineReader::new(right_pipeline, ctx, disk_out, disk_in, block_size)?;

    let combined_schema: Schema = left.out_schema.iter()
        .chain(right.out_schema.iter()).cloned().collect();
    let left_key_idx  = col_idx(&left.out_schema,  left_join_col);
    let right_key_idx = col_idx(&right.out_schema, right_join_col);

    // Initialize both readers — reads first block of each
    left.init(disk_out, disk_in, block_size)?;
    right.init(disk_out, disk_in, block_size)?;

    loop {
        if left.is_exhausted() || right.is_exhausted() { break; }
        let Some(l_row) = left.current()  else { break; };
        let Some(r_row) = right.current() else { break; };

        let l_key = &l_row[left_key_idx];
        let r_key = &r_row[right_key_idx];

        match l_key.partial_cmp(r_key).unwrap_or(std::cmp::Ordering::Equal) {
            std::cmp::Ordering::Less => {
                left.advance(disk_out, disk_in, block_size)?;
            }
            std::cmp::Ordering::Greater => {
                right.advance(disk_out, disk_in, block_size)?;
            }
            std::cmp::Ordering::Equal => {
                let current_key = l_key.clone();

                // Collect right group (all rows with same key) into small buffer
                let mut right_group: Vec<Row> = Vec::new();
                loop {
                    let Some(r) = right.current() else { break; };
                    if r[right_key_idx].partial_cmp(&current_key)
                        != Some(std::cmp::Ordering::Equal) { break; }
                    right_group.push(r);
                    right.advance(disk_out, disk_in, block_size)?;
                }

                // Emit all left rows with same key × right group
                loop {
                    let Some(l) = left.current() else { break; };
                    if l[left_key_idx].partial_cmp(&current_key)
                        != Some(std::cmp::Ordering::Equal) { break; }

                    for r in &right_group {
                        let mut combined = l.clone();
                        combined.extend_from_slice(r);
                        if theta_predicates.iter().all(|p|
                            eval_predicate(&combined, &combined_schema, p))
                        {
                            emit(combined, &combined_schema)?;
                        }
                    }
                    left.advance(disk_out, disk_in, block_size)?;
                }
            }
        }
    }

    Ok(combined_schema)
}

/// Estimate raw table cardinality ignoring filters
fn estimate_scan_cardinality(op: &QueryOp, ctx: &DbContext) -> u64 {
    match op {
        QueryOp::Scan(s) => {
            let table = ctx.get_table_specs().iter()
                .find(|t| t.name == s.table_id);
            if let Some(t) = table {
                for col in &t.column_specs {
                    if let Some(stats) = &col.stats {
                        for stat in stats {
                            if let db_config::statistics::ColumnStat::CardinalityStat(c) = stat {
                                return c.0;
                            }
                        }
                    }
                }
            }
            u64::MAX
        }
        // Strip away filters/projects/sorts — just get the base table size
        QueryOp::Filter(f)  => estimate_scan_cardinality(&f.underlying, ctx),
        QueryOp::Project(p) => estimate_scan_cardinality(&p.underlying, ctx),
        QueryOp::Sort(s)    => estimate_scan_cardinality(&s.underlying, ctx),
        QueryOp::Cross(_)   => u64::MAX, // complex — assume large
    }
}

fn exec_simple_hash_join(
    cross_data:      &CrossData,
    join_predicates: &[common::query::Predicate],
    ctx:             &DbContext,
    disk_out:        &mut impl Write,
    disk_in:         &mut (impl BufRead + Read),
    block_size:      u64,
    allocator:       &mut AnonAllocator,
    emit:            &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    let (left_schema, right_schema, combined_schema, equi_pairs, theta) =
        prepare_join_metadata(cross_data, join_predicates, ctx);

    let left_card  = estimate_scan_cardinality(&cross_data.left, ctx);
    let right_card = estimate_scan_cardinality(&cross_data.right, ctx);
    let build_from_right = right_card <= left_card;

    let (build_side, probe_side, build_schema, probe_schema) = if build_from_right {
        (&cross_data.right, &cross_data.left, &right_schema, &left_schema)
    } else {
        (&cross_data.left, &cross_data.right, &left_schema, &right_schema)
    };

    // Build hash table from smaller side. Support both flat pipeline sources
    // (direct table scans) and arbitrary sub-queries by collecting the build
    // side into memory when necessary.
    use std::collections::HashMap;
    let mut hash_table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();

    // If build side is a flat pipeline (Scan/Filter/Project), stream its
    // blocks directly; otherwise, collect the build side rows via `execute`.
    if let Some(build_pipeline) = try_flatten(build_side) {
        let build_table = ctx.get_table_specs().iter()
            .find(|t| t.name == build_pipeline.scan.table_id).unwrap();
        let build_base: Schema = build_table.column_specs.iter()
            .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
        let build_types: Vec<DataType> = build_base.iter().map(|(_,t)| t.clone()).collect();
        let build_out = pipeline_output_schema(&build_base, &build_pipeline);
        let (build_pre, build_post) = split_filters_by_schema(&build_pipeline.filters, &build_base);
        let bs = disk_get_file_start(disk_out, disk_in, &build_table.file_id)?;
        let bn = disk_get_file_num_blocks(disk_out, disk_in, &build_table.file_id)?;

        let mut bi = 0u64;
        while bi < bn {
            let n = READ_AHEAD_BLOCKS.min(bn - bi);
            let data = disk_read_blocks(disk_out, disk_in, bs + bi, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for row in parse_block(block, &build_types) {
                    if !passes_all_filters(&row, &build_base, &build_pre) { continue; }
                    let row = apply_project_chain(row, &build_base, &build_pipeline.projects);
                    if !passes_all_filters(&row, &build_out, &build_post) { continue; }
                    let mut key_buf = Vec::new();
                    for (li, ri) in &equi_pairs {
                        let idx = if build_from_right { *ri } else { *li };
                        serialize_val_into_buf(&row[idx], &mut key_buf);
                    }
                    hash_table.entry(key_buf).or_default().push(row);
                }
            }
            bi += n;
        }
    } else {
        // Collect arbitrary build-side results into memory (acceptable because
        // we only choose simple-hash when the build side is small).
        let (col_schema, rows, _bytes) = collect_rows(build_side, ctx, disk_out, disk_in, block_size, allocator)?;
        let build_schema = col_schema;
        for row in rows {
            let mut key_buf = Vec::new();
            for (li, ri) in &equi_pairs {
                let idx = if build_from_right { *ri } else { *li };
                serialize_val_into_buf(&row[idx], &mut key_buf);
            }
            hash_table.entry(key_buf).or_default().push(row);
        }
    }

    // Stream probe side. If probe side is a flat pipeline we stream blocks
    // directly; otherwise we execute the probe op and process rows via the
    // executor to avoid special-casing parsing logic.
    if let Some(probe_pipeline) = try_flatten(probe_side) {
        let probe_table = ctx.get_table_specs().iter()
            .find(|t| t.name == probe_pipeline.scan.table_id).unwrap();
        let probe_base: Schema = probe_table.column_specs.iter()
            .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
        let probe_types: Vec<DataType> = probe_base.iter().map(|(_,t)| t.clone()).collect();
        let probe_out = pipeline_output_schema(&probe_base, &probe_pipeline);
        let (probe_pre, probe_post) = split_filters_by_schema(&probe_pipeline.filters, &probe_base);
        let ps = disk_get_file_start(disk_out, disk_in, &probe_table.file_id)?;
        let pn = disk_get_file_num_blocks(disk_out, disk_in, &probe_table.file_id)?;

        let mut pi = 0u64;
        while pi < pn {
            let n = READ_AHEAD_BLOCKS.min(pn - pi);
            let data = disk_read_blocks(disk_out, disk_in, ps + pi, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for row in parse_block(block, &probe_types) {
                    if !passes_all_filters(&row, &probe_base, &probe_pre) { continue; }
                    let probe_row = apply_project_chain(row, &probe_base, &probe_pipeline.projects);
                    if !passes_all_filters(&probe_row, &probe_out, &probe_post) { continue; }
                    let mut key_buf = Vec::new();
                    for (li, ri) in &equi_pairs {
                        let idx = if build_from_right { *li } else { *ri };
                        serialize_val_into_buf(&probe_row[idx], &mut key_buf);
                    }
                    if let Some(build_rows) = hash_table.get(&key_buf) {
                        for build_row in build_rows {
                            let combined: Row = if build_from_right {
                                let mut c = probe_row.clone(); c.extend_from_slice(build_row); c
                            } else {
                                let mut c = build_row.clone(); c.extend_from_slice(&probe_row); c
                            };
                            if theta.iter().all(|p| eval_predicate(&combined, &combined_schema, p)) {
                                emit(combined, &combined_schema)?;
                            }
                        }
                    }
                }
            }
            pi += n;
        }
    } else {
        // Non-flat probe: execute the probe op and process each emitted row.
        execute(probe_side, ctx, disk_out, disk_in, block_size, allocator,
            &mut |probe_row, _schema| {
                // `probe_row` already has projections applied by execute
                let mut key_buf = Vec::new();
                for (li, ri) in &equi_pairs {
                    let idx = if build_from_right { *li } else { *ri };
                    serialize_val_into_buf(&probe_row[idx], &mut key_buf);
                }
                if let Some(build_rows) = hash_table.get(&key_buf) {
                    for build_row in build_rows {
                        let combined: Row = if build_from_right {
                            let mut c = probe_row.clone(); c.extend_from_slice(build_row); c
                        } else {
                            let mut c = build_row.clone(); c.extend_from_slice(&probe_row); c
                        };
                        if theta.iter().all(|p| eval_predicate(&combined, &combined_schema, p)) {
                            emit(combined, &combined_schema)?;
                        }
                    }
                }
                Ok(())
            })?;
    }
    Ok(combined_schema)
}

fn serialize_val(v: &Data) -> Vec<u8> {
    match v {
        Data::Int32(x)   => (*x as i64).to_le_bytes().to_vec(), // Upcast to i64
        Data::Int64(x)   => x.to_le_bytes().to_vec(),
        Data::Float32(x) => (*x as f64).to_le_bytes().to_vec(), // Upcast to f64
        Data::Float64(x) => x.to_le_bytes().to_vec(),
        Data::String(s)  => { let mut b = s.as_bytes().to_vec(); b.push(0); b }
    }
}

fn exec_cross(
    cross_data:      &CrossData,
    join_predicates: &[common::query::Predicate],
    ctx:             &DbContext,
    disk_out:        &mut impl Write,
    disk_in:         &mut (impl BufRead + Read),
    block_size:      u64,
    allocator:       &mut AnonAllocator,
    emit:            &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    let left_schema_preview  = schema_of(&cross_data.left, ctx);
    let right_schema_preview = schema_of(&cross_data.right, ctx);

    // Find equi pairs
    let equi_pairs: Vec<(String, String)> = join_predicates.iter()
        .filter_map(|p| {
            if p.operator != ComparisionOperator::EQ { return None; }
            let ComparisionValue::Column(other) = &p.value else { return None; };
            let lhs_left  = left_schema_preview.iter().any(|(n,_)| n == &p.column_name);
            let rhs_right = right_schema_preview.iter().any(|(n,_)| n == other.as_str());
            let lhs_right = right_schema_preview.iter().any(|(n,_)| n == &p.column_name);
            let rhs_left  = left_schema_preview.iter().any(|(n,_)| n == other.as_str());
            if lhs_left && rhs_right { Some((p.column_name.clone(), other.clone())) }
            else if lhs_right && rhs_left { Some((other.clone(), p.column_name.clone())) }
            else { None }
        })
        .collect();

    let theta: Vec<&common::query::Predicate> = join_predicates.iter()
        .filter(|p| {
            if p.operator != ComparisionOperator::EQ { return true; }
            !matches!(&p.value, ComparisionValue::Column(_))
        })
        .collect();

    if !equi_pairs.is_empty() {
        // Check sort-merge opportunity
        let left_table  = get_scan_table_name(&cross_data.left);
        let right_table = get_scan_table_name(&cross_data.right);
        let left_pipe   = try_flatten(&cross_data.left).is_some();
        let right_pipe  = try_flatten(&cross_data.right).is_some();

        let both_ordered = equi_pairs.len() == 1 && left_pipe && right_pipe &&
            left_table.as_ref().map_or(false, |t|
                is_physically_ordered(&equi_pairs[0].0, t, ctx)) &&
            right_table.as_ref().map_or(false, |t|
                is_physically_ordered(&equi_pairs[0].1, t, ctx));

        if both_ordered {
            return exec_sort_merge_join(
                cross_data, &equi_pairs[0].0, &equi_pairs[0].1,
                &theta, ctx, disk_out, disk_in, block_size, emit,
            );
        }
        // Simple hash join when smaller side fits in memory
        let left_card  = estimate_scan_cardinality(&cross_data.left, ctx);
        let right_card = estimate_scan_cardinality(&cross_data.right, ctx);
        let min_card   = left_card.min(right_card);

        if min_card <= 100000 && try_flatten(&cross_data.left).is_some() 
                            && try_flatten(&cross_data.right).is_some() {
            return exec_simple_hash_join(
                cross_data, join_predicates,
                ctx, disk_out, disk_in, block_size, allocator, emit,
            );
        }

        // Grace hash join — handles any size
        return exec_grace_hash_join(
            cross_data, join_predicates,
            ctx, disk_out, disk_in, block_size, allocator, emit,
        );
    }

    // No equi predicates — nested loop (only for tiny tables like nation×region)
    exec_cross_nested_loop(
        cross_data, join_predicates,
        ctx, disk_out, disk_in, block_size, allocator, emit,
    )
}

// Helper: get the base table name from a pipeline's scan
fn get_scan_table_name(op: &QueryOp) -> Option<String> {
    match op {
        QueryOp::Scan(s)    => Some(s.table_id.clone()),
        QueryOp::Filter(f)  => get_scan_table_name(&f.underlying),
        QueryOp::Project(p) => get_scan_table_name(&p.underlying),
        _                   => None,
    }
}

fn exec_cross_nested_loop(
    cross_data:     &CrossData,
    join_predicates: &[common::query::Predicate], // pushed down from parent Filter
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
    emit:       &mut dyn FnMut(Row,&Schema)->Result<()>,
) -> Result<Schema> {

    // Step 1: Collect right side
    let (right_schema, right_rows, right_bytes) =
        collect_rows(&cross_data.right, ctx, disk_out, disk_in, block_size, allocator)?;

    if right_bytes <= MEMORY_BUDGET_BYTES {
        // ── In-memory path (zero anon disk I/O) ──────────────────────────────
        let right_schema_ref = right_schema.clone();

        let left_schema = execute(
            &cross_data.left, ctx, disk_out, disk_in, block_size, allocator,
            &mut |left_row, left_schema_ref| {
                let combined_schema: Schema = left_schema_ref.iter()
                    .chain(right_schema_ref.iter()).cloned().collect();
                for right_row in &right_rows {
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(right_row);
                    // Apply join predicates inline — avoids emitting rejected pairs
                    if !passes_predicates(&combined, &combined_schema, join_predicates) {
                        continue;
                    }
                    emit(combined, &combined_schema)?;
                }
                Ok(())
            },
        )?;

        let combined_schema: Schema = left_schema.iter()
            .chain(right_schema.iter()).cloned().collect();
        Ok(combined_schema)

    } else {
        // ── Spill path (right side too large for RAM) ─────────────────────────
        // Write collected right rows to anon, load as raw blocks, stream left.
        let (right_start, right_num) =
            spill_rows_to_anon(&right_rows, disk_out, block_size, allocator)?;
        drop(right_rows); // free RAM before loading raw blocks

        let right_col_types: Vec<DataType> = right_schema.iter().map(|(_,t)| t.clone()).collect();

        // Load right as raw blocks (cheaper than Vec<Row>)
        let mut right_blocks: Vec<Vec<u8>> = Vec::with_capacity(right_num as usize);
        let mut ri = 0u64;
        while ri < right_num {
            let n = READ_AHEAD_BLOCKS.min(right_num - ri);
            let data = disk_read_blocks(disk_out, disk_in, right_start + ri, n, block_size)?;
            for b in 0..n as usize {
                let block = data[b * block_size as usize..(b + 1) * block_size as usize].to_vec();
                right_blocks.push(block);
            }
            ri += n;
        }

        let right_schema_ref = right_schema.clone();
        let left_schema = execute(
            &cross_data.left, ctx, disk_out, disk_in, block_size, allocator,
            &mut |left_row, left_schema_ref| {
                let combined_schema: Schema = left_schema_ref.iter()
                    .chain(right_schema_ref.iter()).cloned().collect();
                for right_block in &right_blocks {
                    for right_row in parse_block(right_block, &right_col_types) {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(&right_row);
                        if !passes_predicates(&combined, &combined_schema, join_predicates) {
                            continue;
                        }
                        emit(combined, &combined_schema)?;
                    }
                }
                Ok(())
            },
        )?;

        let combined_schema: Schema = left_schema.iter()
            .chain(right_schema.iter()).cloned().collect();
        Ok(combined_schema)
    }
}

// ── Sort (in-memory fast path + external merge sort fallback) ─────────────────

fn compare_rows(a: &Row, b: &Row, schema: &Schema, specs: &[SortSpec]) -> std::cmp::Ordering {
    for spec in specs {
        let i  =col_idx(schema,&spec.column_name);
        let ord=a[i].partial_cmp(&b[i]).unwrap_or(std::cmp::Ordering::Equal);
        let ord=if spec.ascending{ord}else{ord.reverse()};
        if ord!=std::cmp::Ordering::Equal{return ord;}
    }
    std::cmp::Ordering::Equal
}

// ── Spill any op to anon disk (streaming, no large intermediate buffer) ────────

fn spill_pipeline_to_anon(
    pipeline:   &Pipeline,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
) -> Result<(u64, u64, Schema)> {
    let table = ctx.get_table_specs().iter()
        .find(|t| t.name == pipeline.scan.table_id)
        .with_context(|| format!("Table '{}' not found", pipeline.scan.table_id))?;

    let base_schema: Schema = table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let col_types: Vec<DataType> = base_schema.iter().map(|(_, t)| t.clone()).collect();
    let out_schema = pipeline_output_schema(&base_schema, pipeline);
    let (pre_filters, post_filters) = split_filters_by_schema(&pipeline.filters, &base_schema);
    let file_start = disk_get_file_start(disk_out, disk_in, &table.file_id)?;
    let file_num   = disk_get_file_num_blocks(disk_out, disk_in, &table.file_id)?;

    let anon_start = allocator.next;
    let mut writer = BlockWriter::new(block_size, anon_start);
    let mut fi = 0u64;
    while fi < file_num {
        let n = READ_AHEAD_BLOCKS.min(file_num - fi);
        let data = disk_read_blocks(disk_out, disk_in, file_start + fi, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &col_types) {
                if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
                let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
                if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }
                writer.push(&final_row, disk_out)?;
            }
        }
        fi += n;
    }
    let (sb, nb) = writer.finish(disk_out)?;
    allocator.next = anon_start + nb;
    Ok((sb, nb, out_schema))
}

fn spill_sort_merge_join_to_anon(
    cross_data:    &CrossData,
    left_join_col: &str,
    right_join_col: &str,
    theta:         &[&common::query::Predicate],
    ctx:           &DbContext,
    disk_out:      &mut impl Write,
    disk_in:       &mut (impl BufRead + Read),
    block_size:    u64,
    allocator:     &mut AnonAllocator,
) -> Result<(u64, u64, Schema)> {
    let left_pipeline  = try_flatten(&cross_data.left)
        .expect("sort-merge spill: left must be pipeline");
    let right_pipeline = try_flatten(&cross_data.right)
        .expect("sort-merge spill: right must be pipeline");

    let mut left  = StreamingPipelineReader::new(left_pipeline,  ctx, disk_out, disk_in, block_size)?;
    let mut right = StreamingPipelineReader::new(right_pipeline, ctx, disk_out, disk_in, block_size)?;

    let combined_schema: Schema = left.out_schema.iter()
        .chain(right.out_schema.iter()).cloned().collect();
    let left_key_idx  = col_idx(&left.out_schema,  left_join_col);
    let right_key_idx = col_idx(&right.out_schema, right_join_col);

    left.init(disk_out, disk_in, block_size)?;
    right.init(disk_out, disk_in, block_size)?;

    let anon_start = allocator.next;
    let mut writer = BlockWriter::new(block_size, anon_start);

    loop {
        if left.is_exhausted() || right.is_exhausted() { break; }
        let Some(l_row) = left.current()  else { break; };
        let Some(r_row) = right.current() else { break; };

        let l_key = &l_row[left_key_idx];
        let r_key = &r_row[right_key_idx];

        match l_key.partial_cmp(r_key).unwrap_or(std::cmp::Ordering::Equal) {
            std::cmp::Ordering::Less    => { left.advance(disk_out, disk_in, block_size)?; }
            std::cmp::Ordering::Greater => { right.advance(disk_out, disk_in, block_size)?; }
            std::cmp::Ordering::Equal   => {
                let current_key = l_key.clone();

                let mut right_group: Vec<Row> = Vec::new();
                loop {
                    let Some(r) = right.current() else { break; };
                    if r[right_key_idx].partial_cmp(&current_key)
                        != Some(std::cmp::Ordering::Equal) { break; }
                    right_group.push(r);
                    right.advance(disk_out, disk_in, block_size)?;
                }

                loop {
                    let Some(l) = left.current() else { break; };
                    if l[left_key_idx].partial_cmp(&current_key)
                        != Some(std::cmp::Ordering::Equal) { break; }
                    for r in &right_group {
                        let mut combined = l.clone();
                        combined.extend_from_slice(r);
                        if theta.iter().all(|p| eval_predicate(&combined, &combined_schema, p)) {
                            writer.push_bytes(&serialize_row_bytes(&combined), disk_out)?;
                        }
                    }
                    left.advance(disk_out, disk_in, block_size)?;
                }
            }
        }
    }

    let (sb, nb) = writer.finish(disk_out)?;
    allocator.next = anon_start + nb;
    Ok((sb, nb, combined_schema))
}

// Spill hash join result directly to anon disk.
// Uses a for-loop to probe (not a closure), so disk_out can be used for both
// reading probe blocks AND writing to BlockWriter — no borrow conflict.
// ── Grace Hash Join ───────────────────────────────────────────────────────────

const NUM_PARTITIONS: usize = 256;

fn hash_key(key: &[u8]) -> usize {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % NUM_PARTITIONS
}

/// Grace hash join that EMITS rows (used by exec_cross)
fn exec_grace_hash_join(
    cross_data:       &CrossData,
    join_predicates:  &[common::query::Predicate],
    ctx:              &DbContext,
    disk_out:         &mut impl Write,
    disk_in:          &mut (impl BufRead + Read),
    block_size:       u64,
    allocator:        &mut AnonAllocator,
    emit:             &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    let (left_schema, right_schema, combined_schema, equi_pairs, theta) =
        prepare_join_metadata(cross_data, join_predicates, ctx);

    // Phase 1: partition both sides
    let (left_parts, right_parts) = partition_both_sides(
        cross_data, &equi_pairs, &left_schema, &right_schema,
        ctx, disk_out, disk_in, block_size, allocator,
    )?;

    // Phase 2: join each partition pair
    let left_col_types:  Vec<DataType> = left_schema.iter().map(|(_, t)| t.clone()).collect();
    let right_col_types: Vec<DataType> = right_schema.iter().map(|(_, t)| t.clone()).collect();

    for p in 0..NUM_PARTITIONS {
        let (ls, ln) = left_parts[p];
        let (rs, rn) = right_parts[p];
        if ln == 0 || rn == 0 { continue; }

        // Build hash table from right partition
        use std::collections::HashMap;
        let mut hash_table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
            let mut rj = 0u64;
            while rj < rn {
                let n = READ_AHEAD_BLOCKS.min(rn - rj);
                let data = disk_read_blocks(disk_out, disk_in, rs + rj, n, block_size)?;
                for b in 0..n as usize {
                    let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                    for row in parse_block(block, &right_col_types) {
                        let key = extract_key(&row, &right_schema, &equi_pairs, false);
                        hash_table.entry(key).or_default().push(row);
                    }
                }
                rj += n;
            }

        // Probe with left partition
        let mut lj = 0u64;
        while lj < ln {
            let n = READ_AHEAD_BLOCKS.min(ln - lj);
            let data = disk_read_blocks(disk_out, disk_in, ls + lj, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for left_row in parse_block(block, &left_col_types) {
                    let key = extract_key(&left_row, &left_schema, &equi_pairs, true);
                    if let Some(right_rows) = hash_table.get(&key) {
                        for right_row in right_rows {
                            let mut combined = left_row.clone();
                            combined.extend_from_slice(right_row);
                            if theta.iter().all(|p| eval_predicate(&combined, &combined_schema, p)) {
                                emit(combined, &combined_schema)?;
                            }
                        }
                    }
                }
            }
            lj += n;
        }
    }

    Ok(combined_schema)
}

/// Grace hash join that SPILLS to anon disk (used by Sort over join)
fn spill_grace_hash_join_to_anon(
    cross_data:       &CrossData,
    join_predicates:  &[common::query::Predicate],
    ctx:              &DbContext,
    disk_out:         &mut impl Write,
    disk_in:          &mut (impl BufRead + Read),
    block_size:       u64,
    allocator:        &mut AnonAllocator,
) -> Result<(u64, u64, Schema)> {
    let (left_schema, right_schema, combined_schema, equi_pairs, theta) =
        prepare_join_metadata(cross_data, join_predicates, ctx);

    // Phase 1: partition both sides
    let (left_parts, right_parts) = partition_both_sides(
        cross_data, &equi_pairs, &left_schema, &right_schema,
        ctx, disk_out, disk_in, block_size, allocator,
    )?;

    // Output writer
    let out_start = allocator.next;
    let mut out_writer = BlockWriter::new(block_size, out_start);

    let left_col_types:  Vec<DataType> = left_schema.iter().map(|(_, t)| t.clone()).collect();
    let right_col_types: Vec<DataType> = right_schema.iter().map(|(_, t)| t.clone()).collect();

    // Phase 2: join each partition pair
    for p in 0..NUM_PARTITIONS {
        let (ls, ln) = left_parts[p];
        let (rs, rn) = right_parts[p];
        if ln == 0 || rn == 0 { continue; }

        use std::collections::HashMap;
        let mut hash_table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
        let mut rj = 0u64;
        while rj < rn {
            let n = READ_AHEAD_BLOCKS.min(rn - rj);
            let data = disk_read_blocks(disk_out, disk_in, rs + rj, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for row in parse_block(block, &right_col_types) {
                    let key = extract_key(&row, &right_schema, &equi_pairs, false);
                    hash_table.entry(key).or_default().push(row);
                }
            }
            rj += n;
        }

        let mut lj = 0u64;
        while lj < ln {
            let n = READ_AHEAD_BLOCKS.min(ln - lj);
            let data = disk_read_blocks(disk_out, disk_in, ls + lj, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for left_row in parse_block(block, &left_col_types) {
                    let key = extract_key(&left_row, &left_schema, &equi_pairs, true);
                    if let Some(right_rows) = hash_table.get(&key) {
                        for right_row in right_rows {
                            let mut combined = left_row.clone();
                            combined.extend_from_slice(right_row);
                            if theta.iter().all(|p| eval_predicate(&combined, &combined_schema, p)) {
                                out_writer.push_bytes(&serialize_row_bytes(&combined), disk_out)?;
                            }
                        }
                    }
                }
            }
            lj += n;
        }
    }

    let (sb, nb) = out_writer.finish(disk_out)?;
    allocator.next = out_start + nb;
    Ok((sb, nb, combined_schema))
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn split_filters_by_schema<'a>(
    filters: &[&'a FilterData],
    base_schema: &Schema,
) -> (Vec<&'a FilterData>, Vec<&'a FilterData>) {
    filters.iter().partition(|f| {
        f.predicates.iter().all(|p| {
            let lhs_ok = base_schema.iter().any(|(n,_)| n == &p.column_name);
            let rhs_ok = match &p.value {
                ComparisionValue::Column(c) => base_schema.iter().any(|(n,_)| n == c),
                _ => true,
            };
            lhs_ok && rhs_ok
        })
    })
}

fn prepare_join_metadata<'a>(
    cross_data:      &'a CrossData,
    join_predicates: &'a [common::query::Predicate],
    ctx:             &DbContext,
) -> (Schema, Schema, Schema, Vec<(usize, usize)>, Vec<&'a common::query::Predicate>) {
    let left_schema  = schema_of(&cross_data.left, ctx);
    let right_schema = schema_of(&cross_data.right, ctx);

    let mut equi_pairs: Vec<(usize, usize)> = Vec::new();
    let mut theta: Vec<&common::query::Predicate> = Vec::new();

    for p in join_predicates {
        if p.operator == ComparisionOperator::EQ {
            if let ComparisionValue::Column(other) = &p.value {
                let lhs_left  = left_schema.iter().any(|(n,_)| n == &p.column_name);
                let rhs_right = right_schema.iter().any(|(n,_)| n == other.as_str());
                let lhs_right = right_schema.iter().any(|(n,_)| n == &p.column_name);
                let rhs_left  = left_schema.iter().any(|(n,_)| n == other.as_str());
                if lhs_left && rhs_right {
                    let li = left_schema.iter().position(|(n,_)| n == &p.column_name).unwrap();
                    let ri = right_schema.iter().position(|(n,_)| n == other.as_str()).unwrap();
                    equi_pairs.push((li, ri));
                    continue;
                } else if lhs_right && rhs_left {
                    let li = left_schema.iter().position(|(n,_)| n == other.as_str()).unwrap();
                    let ri = right_schema.iter().position(|(n,_)| n == &p.column_name).unwrap();
                    equi_pairs.push((li, ri));
                    continue;
                }
            }
        }
        theta.push(p);
    }

    let combined_schema: Schema = left_schema.iter()
        .chain(right_schema.iter()).cloned().collect();

    (left_schema, right_schema, combined_schema, equi_pairs, theta)
}

fn extract_key(
    row:        &Row,
    _schema:    &Schema,
    equi_pairs: &[(usize, usize)],
    is_left:    bool,
) -> Vec<u8> {
    let mut key_bytes = Vec::new();
    for (li, ri) in equi_pairs {
        let idx = if is_left { *li } else { *ri };
        key_bytes.extend_from_slice(&serialize_val(&row[idx]));
    }
    key_bytes
}

fn partition_both_sides(
    cross_data:   &CrossData,
    equi_pairs:   &[(usize, usize)],
    left_schema:  &Schema,
    right_schema: &Schema,
    ctx:          &DbContext,
    disk_out:     &mut impl Write,
    disk_in:      &mut (impl BufRead + Read),
    block_size:   u64,
    allocator:    &mut AnonAllocator,
) -> Result<([(u64, u64); NUM_PARTITIONS], [(u64, u64); NUM_PARTITIONS])> {
    // Allocate block IDs for all partition writers upfront
    // Each partition writer starts at a pre-allocated position
    // We use a large sparse region — disk simulator allocates lazily
    // Allocate block IDs for all partition writers upfront
    let left_partition_starts: Vec<u64> = (0..NUM_PARTITIONS)
        .map(|_| { let s = allocator.next; allocator.next += 100_000; s }) // Increased to 100k
        .collect();
    let right_partition_starts: Vec<u64> = (0..NUM_PARTITIONS)
        .map(|_| { let s = allocator.next; allocator.next += 100_000; s }) // Increased to 100k
        .collect();

    // Partition left side
    let mut left_writers: Vec<BlockWriter> = left_partition_starts.iter()
        .map(|&s| BlockWriter::new(block_size, s))
        .collect();

    partition_op(
        &cross_data.left, equi_pairs, true, left_schema,
        ctx, disk_out, disk_in, block_size, allocator, &mut left_writers,
    )?;

    // Flush left writers and record (start, num_blocks)
    let mut left_parts = [(0u64, 0u64); NUM_PARTITIONS];
    for (i, writer) in left_writers.into_iter().enumerate() {
        let (sb, nb) = writer.finish(disk_out)?;
        left_parts[i] = (sb, nb);
        // Update allocator to actual usage
        if nb > 0 {
            allocator.next = allocator.next.max(sb + nb);
        }
    }

    // Partition right side
    let mut right_writers: Vec<BlockWriter> = right_partition_starts.iter()
        .map(|&s| BlockWriter::new(block_size, s))
        .collect();

    partition_op(
        &cross_data.right, equi_pairs, false, right_schema,
        ctx, disk_out, disk_in, block_size, allocator, &mut right_writers,
    )?;

    let mut right_parts = [(0u64, 0u64); NUM_PARTITIONS];
    for (i, writer) in right_writers.into_iter().enumerate() {
        let (sb, nb) = writer.finish(disk_out)?;
        right_parts[i] = (sb, nb);
        if nb > 0 {
            allocator.next = allocator.next.max(sb + nb);
        }
    }

    Ok((left_parts, right_parts))
}

fn partition_op(
    op:          &QueryOp,
    equi_pairs:  &[(usize, usize)],
    is_left:     bool,
    schema:      &Schema,
    ctx:         &DbContext,
    disk_out:    &mut impl Write,
    disk_in:     &mut (impl BufRead + Read),
    block_size:  u64,
    allocator:   &mut AnonAllocator,
    writers:     &mut Vec<BlockWriter>,
) -> Result<()> {
    // Fast path: pipeline
    if let Some(pipeline) = try_flatten(op) {
        return partition_pipeline(&pipeline, equi_pairs, is_left, schema,
            ctx, disk_out, disk_in, block_size, writers);
    }

    // Special case: Cross(pipeline, pipeline) — load right in RAM, stream left
    if let QueryOp::Cross(c) = op {
        if let (Some(left_pipe), Some(right_pipe)) =
            (try_flatten(&c.left), try_flatten(&c.right))
        {
            return partition_cross_pipelines(
                &left_pipe, &right_pipe, equi_pairs, is_left,
                ctx, disk_out, disk_in, block_size, writers);
        }
    }

    // General case: spill op to anon disk first, then read back and partition
    // This avoids holding large intermediate results in RAM
    let (spill_start, spill_num, spill_schema) =
        spill_hash_join_to_anon_generic(op, ctx, disk_out, disk_in, block_size, allocator)?;

    let col_types: Vec<DataType> = spill_schema.iter().map(|(_, t)| t.clone()).collect();

    let mut si = 0u64;
    while si < spill_num {
        let n = READ_AHEAD_BLOCKS.min(spill_num - si);
        let data = disk_read_blocks(disk_out, disk_in, spill_start + si, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &col_types) {
                let key = extract_key(&row, &spill_schema, equi_pairs, is_left);
                let p = hash_key(&key);
                writers[p].push(&row, disk_out)?;
            }
        }
        si += n;
    }
    Ok(())
}

// Extracted helper for Cross(pipeline, pipeline) case
fn partition_cross_pipelines(
    left_pipe:  &Pipeline,
    right_pipe: &Pipeline,
    equi_pairs: &[(usize, usize)],
    is_left:    bool,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    writers:    &mut Vec<BlockWriter>,
) -> Result<()> {
    let right_table = ctx.get_table_specs().iter()
        .find(|t| t.name == right_pipe.scan.table_id)
        .with_context(|| format!("Table '{}' not found", right_pipe.scan.table_id))?;
    let right_base: Schema = right_table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let right_types: Vec<DataType> = right_base.iter().map(|(_,t)| t.clone()).collect();
    let right_out = pipeline_output_schema(&right_base, right_pipe);
    let rs = disk_get_file_start(disk_out, disk_in, &right_table.file_id)?;
    let rn = disk_get_file_num_blocks(disk_out, disk_in, &right_table.file_id)?;

    let mut right_rows: Vec<Row> = Vec::new();
    let mut rpi = 0u64;
    while rpi < rn {
        let n = READ_AHEAD_BLOCKS.min(rn - rpi);
        let data = disk_read_blocks(disk_out, disk_in, rs + rpi, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &right_types) {
                if !passes_all_filters(&row, &right_base, &right_pipe.filters) { continue; }
                right_rows.push(apply_project_chain(row, &right_base, &right_pipe.projects));
            }
        }
        rpi += n;
    }

    let left_table = ctx.get_table_specs().iter()
        .find(|t| t.name == left_pipe.scan.table_id)
        .with_context(|| format!("Table '{}' not found", left_pipe.scan.table_id))?;
    let left_base: Schema = left_table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let left_types: Vec<DataType> = left_base.iter().map(|(_,t)| t.clone()).collect();
    let left_out = pipeline_output_schema(&left_base, left_pipe);
    let combined_schema: Schema = left_out.iter()
        .chain(right_out.iter()).cloned().collect();
    let ls = disk_get_file_start(disk_out, disk_in, &left_table.file_id)?;
    let ln = disk_get_file_num_blocks(disk_out, disk_in, &left_table.file_id)?;

    let mut lpi = 0u64;
    while lpi < ln {
        let n = READ_AHEAD_BLOCKS.min(ln - lpi);
        let data = disk_read_blocks(disk_out, disk_in, ls + lpi, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for left_row in parse_block(block, &left_types) {
                if !passes_all_filters(&left_row, &left_base, &left_pipe.filters) { continue; }
                let l = apply_project_chain(left_row, &left_base, &left_pipe.projects);
                for r in &right_rows {
                    let mut combined = l.clone();
                    combined.extend_from_slice(r);
                    let key = extract_key(&combined, &combined_schema, equi_pairs, is_left);
                    let p = hash_key(&key);
                    writers[p].push(&combined, disk_out)?;
                }
            }
        }
        lpi += n;
    }
    Ok(())
}

fn partition_pipeline(
    pipeline:    &Pipeline,
    equi_pairs:  &[(usize, usize)],
    is_left:     bool,
    _schema:     &Schema,
    ctx:         &DbContext,
    disk_out:    &mut impl Write,
    disk_in:     &mut (impl BufRead + Read),
    block_size:  u64,
    writers:     &mut Vec<BlockWriter>,
) -> Result<()> {
    let table = ctx.get_table_specs().iter()
        .find(|t| t.name == pipeline.scan.table_id)
        .with_context(|| format!("Table '{}' not found", pipeline.scan.table_id))?;

    let base_schema: Schema = table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let col_types: Vec<DataType> = base_schema.iter().map(|(_, t)| t.clone()).collect();
    let out_schema = pipeline_output_schema(&base_schema, pipeline);
    let (pre_filters, post_filters) = split_filters_by_schema(&pipeline.filters, &base_schema);

    let start = disk_get_file_start(disk_out, disk_in, &table.file_id)?;
    let num   = disk_get_file_num_blocks(disk_out, disk_in, &table.file_id)?;

    let mut idx = 0u64;
    while idx < num {
        let n = READ_AHEAD_BLOCKS.min(num - idx);
        let data = disk_read_blocks(disk_out, disk_in, start + idx, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &col_types) {
                if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
                let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
                if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }
                let key = extract_key(&final_row, &out_schema, equi_pairs, is_left);
                let p = hash_key(&key);
                writers[p].push(&final_row, disk_out)?;
            }
        }
        idx += n;
    }
    Ok(())
}

fn spill_simple_hash_join_to_anon(
    cross_data:      &CrossData,
    join_predicates: &[common::query::Predicate],
    ctx:             &DbContext,
    disk_out:        &mut impl Write,
    disk_in:         &mut (impl BufRead + Read),
    block_size:      u64,
    allocator:       &mut AnonAllocator,
) -> Result<(u64, u64, Schema)> {
    let (left_schema, right_schema, combined_schema, equi_pairs, theta) =
        prepare_join_metadata(cross_data, join_predicates, ctx);

    let left_card  = estimate_scan_cardinality(&cross_data.left, ctx);
    let right_card = estimate_scan_cardinality(&cross_data.right, ctx);
    let build_from_right = right_card <= left_card;

    let (build_side, probe_side, build_schema, probe_schema) = if build_from_right {
        (&cross_data.right, &cross_data.left, &right_schema, &left_schema)
    } else {
        (&cross_data.left, &cross_data.right, &left_schema, &right_schema)
    };

    // Build hash table from smaller side
    use std::collections::HashMap;
    let mut hash_table: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();

    let build_pipeline = try_flatten(build_side).unwrap();
    let build_table = ctx.get_table_specs().iter()
        .find(|t| t.name == build_pipeline.scan.table_id).unwrap();
    let build_base: Schema = build_table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let build_types: Vec<DataType> = build_base.iter().map(|(_,t)| t.clone()).collect();
    let build_out = pipeline_output_schema(&build_base, &build_pipeline);
    let (build_pre, build_post) = split_filters_by_schema(&build_pipeline.filters, &build_base);
    let bs = disk_get_file_start(disk_out, disk_in, &build_table.file_id)?;
    let bn = disk_get_file_num_blocks(disk_out, disk_in, &build_table.file_id)?;

    let mut bi = 0u64;
    while bi < bn {
        let n = READ_AHEAD_BLOCKS.min(bn - bi);
        let data = disk_read_blocks(disk_out, disk_in, bs + bi, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &build_types) {
                if !passes_all_filters(&row, &build_base, &build_pre) { continue; }
                let row = apply_project_chain(row, &build_base, &build_pipeline.projects);
                if !passes_all_filters(&row, &build_out, &build_post) { continue; }
                let mut key_buf = Vec::new();
                for (li, ri) in &equi_pairs {
                    let idx = if build_from_right { *ri } else { *li };
                    serialize_val_into_buf(&row[idx], &mut key_buf);
                }
                hash_table.entry(key_buf).or_default().push(row);
            }
        }
        bi += n;
    }

    // Probe side — stream it
    let probe_pipeline = try_flatten(probe_side).unwrap();
    let probe_table = ctx.get_table_specs().iter()
        .find(|t| t.name == probe_pipeline.scan.table_id).unwrap();
    let probe_base: Schema = probe_table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let probe_types: Vec<DataType> = probe_base.iter().map(|(_,t)| t.clone()).collect();
    let probe_out = pipeline_output_schema(&probe_base, &probe_pipeline);
    let (probe_pre, probe_post) = split_filters_by_schema(&probe_pipeline.filters, &probe_base);
    let ps = disk_get_file_start(disk_out, disk_in, &probe_table.file_id)?;
    let pn = disk_get_file_num_blocks(disk_out, disk_in, &probe_table.file_id)?;

    let combined_schema_final: Schema = if build_from_right {
        probe_schema.iter().chain(build_schema.iter()).cloned().collect()
    } else {
        build_schema.iter().chain(probe_schema.iter()).cloned().collect()
    };

    let out_start = allocator.next;
    let mut writer = BlockWriter::new(block_size, out_start);

    let mut pi = 0u64;
    while pi < pn {
        let n = READ_AHEAD_BLOCKS.min(pn - pi);
        let data = disk_read_blocks(disk_out, disk_in, ps + pi, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &probe_types) {
                if !passes_all_filters(&row, &probe_base, &probe_pre) { continue; }
                let probe_row = apply_project_chain(row, &probe_base, &probe_pipeline.projects);
                if !passes_all_filters(&probe_row, &probe_out, &probe_post) { continue; }
                let mut key_buf = Vec::new();
                for (li, ri) in &equi_pairs {
                    let idx = if build_from_right { *li } else { *ri };
                    serialize_val_into_buf(&probe_row[idx], &mut key_buf);
                }
                if let Some(build_rows) = hash_table.get(&key_buf) {
                    for build_row in build_rows {
                        let combined: Row = if build_from_right {
                            let mut c = probe_row.clone(); c.extend_from_slice(build_row); c
                        } else {
                            let mut c = build_row.clone(); c.extend_from_slice(&probe_row); c
                        };
                        if theta.iter().all(|p| eval_predicate(&combined, &combined_schema_final, p)) {
                            writer.push_bytes(&serialize_row_bytes(&combined), disk_out)?;
                        }
                    }
                }
            }
        }
        pi += n;
    }

    let (sb, nb) = writer.finish(disk_out)?;
    allocator.next = out_start + nb;
    Ok((sb, nb, combined_schema_final))
}

// Generic spill for any op — used for nested Cross children
fn spill_hash_join_to_anon_generic(
    op:         &QueryOp,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
) -> Result<(u64, u64, Schema)> {
    if let Some(pipe) = try_flatten(op) {
        return spill_pipeline_to_anon(&pipe, ctx, disk_out, disk_in, block_size, allocator);
    }

    // NEW: Peel off an optional Project wrapper to expose the underlying join
    let inner_op = match op {
        QueryOp::Project(p) => p.underlying.as_ref(),
        _ => op,
    };

    if let QueryOp::Filter(f) = inner_op {
        if let QueryOp::Cross(c) = f.underlying.as_ref() {
            // Check sort-merge first
            let equi_pairs: Vec<(String,String)> = f.predicates.iter()
                .filter_map(|p| {
                    if p.operator != ComparisionOperator::EQ { return None; }
                    let ComparisionValue::Column(other) = &p.value else { return None; };
                    let ls = schema_of(&c.left, ctx);
                    let rs = schema_of(&c.right, ctx);
                    let lhs_left = ls.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_right = rs.iter().any(|(n,_)| n == other.as_str());
                    if lhs_left && rhs_right { Some((p.column_name.clone(), other.clone())) }
                    else { None }
                }).collect();

            if equi_pairs.len() == 1 {
                let lt = get_scan_table_name(&c.left);
                let rt = get_scan_table_name(&c.right);
                let lp = try_flatten(&c.left).is_some();
                let rp = try_flatten(&c.right).is_some();
                if lp && rp &&
                    lt.as_ref().map_or(false, |t| is_physically_ordered(&equi_pairs[0].0, t, ctx)) &&
                    rt.as_ref().map_or(false, |t| is_physically_ordered(&equi_pairs[0].1, t, ctx))
                {
                    return spill_sort_merge_join_to_anon(
                        c, &equi_pairs[0].0, &equi_pairs[0].1,
                        &f.predicates.iter()
                            .filter(|p| !matches!(&p.value, ComparisionValue::Column(_)))
                            .collect::<Vec<_>>(),
                        ctx, disk_out, disk_in, block_size, allocator,
                    );
                }
            }
            let left_card  = estimate_scan_cardinality(&c.left, ctx);
            let right_card = estimate_scan_cardinality(&c.right, ctx);
            let lp = try_flatten(&c.left).is_some();
            let rp = try_flatten(&c.right).is_some();

            if lp && rp && left_card.min(right_card) <= 100000 {
                return spill_simple_hash_join_to_anon(
                    c, &f.predicates,
                    ctx, disk_out, disk_in, block_size, allocator,
                );
            }

            return spill_grace_hash_join_to_anon(
                c, &f.predicates,
                ctx, disk_out, disk_in, block_size, allocator,
            );
        }
    }
    if let QueryOp::Cross(c) = inner_op {
        return spill_grace_hash_join_to_anon(
            c, &[], ctx, disk_out, disk_in, block_size, allocator,
        );
    }
    // Fallback for small results
    let mut rows: Vec<Row> = Vec::new();
    let mut schema_out: Option<Schema> = None;
    let child_schema = execute(op, ctx, disk_out, disk_in, block_size, allocator,
        &mut |row, schema| {
            if schema_out.is_none() { schema_out = Some(schema.clone()); }
            rows.push(row);
            Ok(())
        })?;
    
    let schema = schema_out.unwrap_or(child_schema);
    let start = allocator.next;
    let mut writer = BlockWriter::new(block_size, start);
    for row in &rows { writer.push(row, disk_out)?; }
    let (sb, nb) = writer.finish(disk_out)?;
    allocator.next = start + nb;
    Ok((sb, nb, schema))
}

fn exec_sort(
    sort_data:  &SortData,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
    emit:       &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    if let Some(pipeline) = try_flatten(&sort_data.underlying) {
        return exec_sort_from_pipeline(
            &pipeline, sort_data, ctx, disk_out, disk_in, block_size, allocator, emit
        );
    }
    

    let (child_start, child_num, child_schema) =
        spill_hash_join_to_anon_generic(
            &sort_data.underlying, ctx, disk_out, disk_in, block_size, allocator
        )?;
    

    if child_num == 0 { return Ok(child_schema); }

    let col_types: Vec<DataType> = child_schema.iter().map(|(_, t)| t.clone()).collect();
    let mut runs: Vec<(u64, u64)> = Vec::new();
    let mut block_idx = 0u64;

    while block_idx < child_num {
        // Accumulate rows block by block until budget exceeded
        // Track RUST object size, not raw bytes
        let mut batch: Vec<Row> = Vec::new();
        let mut batch_bytes: usize = 0;
        let mut blocks_read: u64 = 0;

        while block_idx + blocks_read < child_num {
            let remaining = child_num - (block_idx + blocks_read);
            let n = READ_AHEAD_BLOCKS.min(remaining);
            let data = disk_read_blocks(disk_out, disk_in, child_start + block_idx + blocks_read, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                let new_rows = parse_block(block, &col_types);
                blocks_read += 1;
                for row in new_rows {
                    batch_bytes += rust_row_size(&row);
                    batch.push(row);
                }
                if batch_bytes >= SORT_RUN_BYTES { break; }
            }
            if batch_bytes >= SORT_RUN_BYTES { break; }
        }
        

        block_idx += blocks_read;

        if batch.is_empty() { break; }

        batch.sort_by(|a, b| compare_rows(a, b, &child_schema, &sort_data.sort_specs));

        // Only run — emit directly, zero extra disk writes
        if runs.is_empty() && block_idx >= child_num {
            for row in batch { emit(row, &child_schema)?; }
            return Ok(child_schema);
        }

        let (rs, rn) = spill_rows_to_anon(&batch, disk_out, block_size, allocator)?;
        if rn > 0 { runs.push((rs, rn)); }
    }

    if runs.len() == 1 {
        // Single run — read back sequentially
        let (rs, rn) = runs[0];
        let mut ri = 0u64;
        while ri < rn {
            let n = READ_AHEAD_BLOCKS.min(rn - ri);
            let data = disk_read_blocks(disk_out, disk_in, rs + ri, n, block_size)?;
            for b in 0..n as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                for row in parse_block(block, &col_types) {
                    emit(row, &child_schema)?;
                }
            }
            ri += n;
        }
    } else {
        
        merge_runs(runs, &child_schema, sort_data, disk_out, disk_in, block_size, emit)?;
    }

    Ok(child_schema)
}

fn exec_sort_from_pipeline(
    pipeline:   &Pipeline,
    sort_data:  &SortData,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
    emit:       &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<Schema> {
    let table = ctx.get_table_specs().iter()
        .find(|t| t.name == pipeline.scan.table_id)
        .with_context(|| format!("Table '{}' not found", pipeline.scan.table_id))?;

    let base_schema: Schema = table.column_specs.iter()
        .map(|c| (c.column_name.clone(), c.data_type.clone())).collect();
    let col_types: Vec<DataType> = base_schema.iter().map(|(_, t)| t.clone()).collect();
    let out_schema = pipeline_output_schema(&base_schema, pipeline);
    let (pre_filters, post_filters) = split_filters_by_schema(&pipeline.filters, &base_schema);

    let start = disk_get_file_start(disk_out, disk_in, &table.file_id)?;
    let num   = disk_get_file_num_blocks(disk_out, disk_in, &table.file_id)?;

    let mut runs: Vec<(u64, u64)> = Vec::new();
    let mut batch: Vec<Row> = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut total_rows: usize = 0;

    let mut idx = 0u64;
    while idx < num {
        let n = READ_AHEAD_BLOCKS.min(num - idx);
        let data = disk_read_blocks(disk_out, disk_in, start + idx, n, block_size)?;
        for b in 0..n as usize {
            let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
            for row in parse_block(block, &col_types) {
                if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
                let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
                if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }

                batch_bytes += rust_row_size(&final_row);
                batch.push(final_row);
                total_rows += 1;

                if batch_bytes > SORT_RUN_BYTES {
                    let precomp: Vec<(usize, bool)> = sort_data.sort_specs.iter()
                        .map(|s| (col_idx(&out_schema, &s.column_name), s.ascending)).collect();
                    batch.sort_by(|a, b| {
                        for (i, asc) in &precomp {
                            let ord = a[*i].partial_cmp(&b[*i]).unwrap_or(std::cmp::Ordering::Equal);
                            let ord = if *asc { ord } else { ord.reverse() };
                            if ord != std::cmp::Ordering::Equal { return ord; }
                        }
                        std::cmp::Ordering::Equal
                    });
                    let (rs, rn) = spill_rows_to_anon(&batch, disk_out, block_size, allocator)?;
                    if rn > 0 { runs.push((rs, rn)); }
                    batch.clear();
                    batch_bytes = 0;
                }
            }
        }
        idx += n;
    }

    

    if runs.is_empty() {
        let precomp: Vec<(usize, bool)> = sort_data.sort_specs.iter()
            .map(|s| (col_idx(&out_schema, &s.column_name), s.ascending)).collect();
        batch.sort_by(|a, b| {
            for (i, asc) in &precomp {
                let ord = a[*i].partial_cmp(&b[*i]).unwrap_or(std::cmp::Ordering::Equal);
                let ord = if *asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal { return ord; }
            }
            std::cmp::Ordering::Equal
        });
        for row in batch { emit(row, &out_schema)?; }
        return Ok(out_schema);
    }

    if !batch.is_empty() {
        let precomp: Vec<(usize, bool)> = sort_data.sort_specs.iter()
            .map(|s| (col_idx(&out_schema, &s.column_name), s.ascending)).collect();
        batch.sort_by(|a, b| {
            for (i, asc) in &precomp {
                let ord = a[*i].partial_cmp(&b[*i]).unwrap_or(std::cmp::Ordering::Equal);
                let ord = if *asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal { return ord; }
            }
            std::cmp::Ordering::Equal
        });
        let (rs, rn) = spill_rows_to_anon(&batch, disk_out, block_size, allocator)?;
        if rn > 0 { runs.push((rs, rn)); }
    }

    merge_runs(runs, &out_schema, sort_data, disk_out, disk_in, block_size, emit)?;
    Ok(out_schema)
}

fn merge_runs(
    runs:       Vec<(u64, u64)>,
    schema:     &Schema,
    sort_data:  &SortData,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    emit:       &mut dyn FnMut(Row, &Schema) -> Result<()>,
) -> Result<()> {
    use std::collections::BinaryHeap;

    let col_types: Vec<DataType> = schema.iter().map(|(_, t)| t.clone()).collect();

    // Pre-compile sort spec indices once
    let comp_sort: Vec<(usize, bool)> = sort_data.sort_specs.iter()
        .map(|s| (schema.iter().position(|(n,_)| n == &s.column_name).unwrap(), s.ascending))
        .collect();

    struct HeapEntry {
        row:       Row,
        run_idx:   usize,
        comp_sort: *const Vec<(usize, bool)>,
    }
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool { self.cmp(other) == std::cmp::Ordering::Equal }
    }
    impl Eq for HeapEntry {}
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            let specs = unsafe { &*self.comp_sort };
            // REVERSED: BinaryHeap is max-heap, we want min
            for &(i, ascending) in specs {
                let ord = other.row[i].partial_cmp(&self.row[i]).unwrap_or(std::cmp::Ordering::Equal);
                let ord = if ascending { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal { return ord; }
            }
            std::cmp::Ordering::Equal
        }
    }

    let mut states: Vec<RunState> = Vec::with_capacity(runs.len());
    for (rs, rn) in &runs {
        if *rn == 0 { continue; }
        let mut state = RunState {
            start_block: *rs, num_blocks: *rn,
            cur_block: 0, rows: std::collections::VecDeque::new(),
        };
        state.advance(disk_out, disk_in, block_size, &col_types)?;
        states.push(state);
    }

    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(states.len());
    for (i, state) in states.iter().enumerate() {
        if let Some(row) = state.peek() {
            heap.push(HeapEntry { row: row.clone(), run_idx: i, comp_sort: &comp_sort });
        }
    }

    while let Some(entry) = heap.pop() {
        emit(entry.row, schema)?;
        let i = entry.run_idx;
        states[i].advance(disk_out, disk_in, block_size, &col_types)?;
        if let Some(row) = states[i].peek() {
            heap.push(HeapEntry { row: row.clone(), run_idx: i, comp_sort: &comp_sort });
        }
    }
    Ok(())
}


struct RunState {
    start_block: u64, num_blocks: u64, cur_block: u64,
    rows: std::collections::VecDeque<Row>, // Changed from Vec to VecDeque
}

impl RunState {
    fn peek(&self) -> Option<&Row> {
        self.rows.front()
    }

    fn advance(&mut self, disk_out: &mut impl Write, disk_in: &mut (impl BufRead+Read),
               block_size: u64, col_types: &[DataType]) -> Result<()> {
        self.rows.pop_front();
        
        if self.rows.is_empty() && self.cur_block < self.num_blocks {
            // During k-way merge we only prefetch a small number of blocks per
            // run to avoid a large memory spike when many runs are open.
            let blocks_to_read = MERGE_PREFETCH_BLOCKS.min(self.num_blocks - self.cur_block);
            let data = disk_read_blocks(disk_out, disk_in, self.start_block + self.cur_block, blocks_to_read, block_size)?;
            for b in 0..blocks_to_read as usize {
                let block = &data[b * block_size as usize..(b + 1) * block_size as usize];
                self.rows.extend(parse_block(block, col_types));
                self.cur_block += 1;
            }
        }
        Ok(())
    }
}

// ── Core execute ──────────────────────────────────────────────────────────────
//
// KEY OPTIMIZATION: detect Filter(join_predicates, Cross(...)) pattern and
// apply join predicates INSIDE the cross loop rather than after.
// This avoids generating rejected pairs entirely.

fn execute(
    op:         &QueryOp,
    ctx:        &DbContext,
    disk_out:   &mut impl Write,
    disk_in:    &mut (impl BufRead + Read),
    block_size: u64,
    allocator:  &mut AnonAllocator,
    emit:       &mut dyn FnMut(Row,&Schema)->Result<()>,
) -> Result<Schema> {
    // Fast path: Scan/Filter/Project pipeline
    if let Some(pipeline)=try_flatten(op) {
        return exec_pipeline(&pipeline,ctx,disk_out,disk_in,block_size,emit);
    }

    match op {
        QueryOp::Sort(s) =>
            exec_sort(s,ctx,disk_out,disk_in,block_size,allocator,emit),

        // KEY: detect Filter-over-Cross and push predicates into join loop
        QueryOp::Filter(f) if matches!(f.underlying.as_ref(), QueryOp::Cross(_)) => {
            if let QueryOp::Cross(cross_data) = f.underlying.as_ref() {
                exec_cross(cross_data, &f.predicates, ctx, disk_out, disk_in,
                           block_size, allocator, emit)
            } else { unreachable!() }
        }

        QueryOp::Filter(f) => {
            execute(&f.underlying,ctx,disk_out,disk_in,block_size,allocator,
                &mut |row,schema| {
                    if passes_all_filters(&row,schema,&[f]) { emit(row,schema)?; }
                    Ok(())
                })
        }

        QueryOp::Cross(c) =>
            exec_cross(c,&[],ctx,disk_out,disk_in,block_size,allocator,emit),

        QueryOp::Project(p) => {
            let mut out_schema=Schema::new();
            let child_schema=execute(&p.underlying,ctx,disk_out,disk_in,block_size,allocator,
                &mut |row,schema| {
                    let nr=project_row(&row,schema,p);
                    if out_schema.is_empty(){out_schema=project_schema(schema,p);}
                    emit(nr,&out_schema)?;
                    Ok(())
                })?;
            if out_schema.is_empty(){out_schema=project_schema(&child_schema,p);}
            Ok(out_schema)
        }

        QueryOp::Scan(_) => unreachable!("Scan must be caught by pipeline"),
    }
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

fn schema_of(op: &QueryOp, ctx: &DbContext) -> Schema {
    match op {
        QueryOp::Scan(s) => {
            let t=ctx.get_table_specs().iter().find(|t|t.name==s.table_id).unwrap();
            t.column_specs.iter().map(|c|(c.column_name.clone(),c.data_type.clone())).collect()
        }
        QueryOp::Filter(f)  => schema_of(&f.underlying,ctx),
        QueryOp::Sort(s)    => schema_of(&s.underlying,ctx),
        QueryOp::Project(p) => { let c=schema_of(&p.underlying,ctx); project_schema(&c,p) }
        QueryOp::Cross(c)   => { let mut s=schema_of(&c.left,ctx); s.extend(schema_of(&c.right,ctx)); s }
    }
}

fn pred_refs_schema(pred: &common::query::Predicate, schema: &Schema) -> bool {
    let has=|name:&str| schema.iter().any(|(n,_)|n==name);
    has(&pred.column_name)||matches!(&pred.value,ComparisionValue::Column(c) if has(c))
}

fn push_filter_down(
    predicates: Vec<common::query::Predicate>, child: QueryOp, ctx: &DbContext,
) -> QueryOp {
    match child {
        QueryOp::Sort(s) => {
            let pushed=push_filter_down(predicates,*s.underlying,ctx);
            QueryOp::Sort(SortData{underlying:Box::new(pushed),sort_specs:s.sort_specs})
        }
        QueryOp::Filter(f) => {
            let mut all=predicates; all.extend(f.predicates);
            push_filter_down(all,*f.underlying,ctx)
        }
        QueryOp::Project(p) => {
            // Only push predicates that reference pre-projection column names
            let child_schema = schema_of(&p.underlying, ctx);
            let (pushable, stay): (Vec<_>, Vec<_>) = predicates.into_iter().partition(|pred| {
                let lhs_ok = child_schema.iter().any(|(n,_)| n == &pred.column_name);
                let rhs_ok = match &pred.value {
                    ComparisionValue::Column(c) => child_schema.iter().any(|(n,_)| n == c),
                    _ => true,
                };
                lhs_ok && rhs_ok
            });

            let new_underlying = if pushable.is_empty() {
                *p.underlying
            } else {
                push_filter_down(pushable, *p.underlying, ctx)
            };

            let new_project = QueryOp::Project(ProjectData {
                underlying: Box::new(new_underlying),
                column_name_map: p.column_name_map,
            });

            if stay.is_empty() {
                new_project
            } else {
                QueryOp::Filter(FilterData {
                    predicates: stay,
                    underlying: Box::new(new_project),
                })
            }
        }
        QueryOp::Cross(c) => {
            let ls=schema_of(&c.left,ctx); let rs=schema_of(&c.right,ctx);
            let (mut lp,mut rp,mut bp)=(Vec::new(),Vec::new(),Vec::new());
            for pred in predicates {
                match (pred_refs_schema(&pred,&ls),pred_refs_schema(&pred,&rs)) {
                    (true,false)=>lp.push(pred),
                    (false,true)=>rp.push(pred),
                    _           =>bp.push(pred),
                }
            }
            let nl=if lp.is_empty(){*c.left}else{push_filter_down(lp,*c.left,ctx)};
            let nr=if rp.is_empty(){*c.right}else{push_filter_down(rp,*c.right,ctx)};
            let cross=QueryOp::Cross(CrossData{left:Box::new(nl),right:Box::new(nr)});
            if bp.is_empty(){cross}
            else{QueryOp::Filter(FilterData{predicates:bp,underlying:Box::new(cross)})}
        }
        other=>QueryOp::Filter(FilterData{predicates,underlying:Box::new(other)}),
    }
}

fn flatten_cross_tree_owned(op: QueryOp) -> Vec<QueryOp> {
    match op {
        QueryOp::Cross(c) => {
            let mut left = flatten_cross_tree_owned(*c.left);
            left.extend(flatten_cross_tree_owned(*c.right));
            left
        }
        other => vec![other],
    }
}

/// Reorder a multi-way join from smallest to largest table
/// Detects Filter(predicates, Cross(Cross(...))) and rebuilds optimally
fn build_incremental_join(
    tables:     Vec<QueryOp>,
    predicates: Vec<common::query::Predicate>,
    ctx:        &DbContext,
) -> QueryOp {
    if tables.len() == 1 {
        let t = tables.into_iter().next().unwrap();
        return if predicates.is_empty() { t }
        else { QueryOp::Filter(FilterData { predicates, underlying: Box::new(t) }) };
    }

    // Sort by cardinality initially
    let mut remaining: Vec<(QueryOp, u64)> = tables.into_iter()
        .map(|t| { let c = estimate_scan_cardinality(&t, ctx); (t, c) })
        .collect();
    remaining.sort_by_key(|(_,c)| *c);

    let mut remaining_preds = predicates;

    // Pick first table (smallest)
    let (first, _) = remaining.remove(0);
    let mut current_schema = schema_of(&first, ctx);

    // Apply literal predicates to first table immediately (e.g. r_name='ASIA')
    let (first_filters, rest): (Vec<_>, Vec<_>) =
        remaining_preds.into_iter().partition(|p| {
            let lhs_ok = current_schema.iter().any(|(n,_)| n == &p.column_name);
            !matches!(&p.value, ComparisionValue::Column(_)) && lhs_ok
        });
    remaining_preds = rest;

    let mut current = if first_filters.is_empty() { first }
    else { QueryOp::Filter(FilterData {
        predicates: first_filters,
        underlying: Box::new(first),
    })};

    while !remaining.is_empty() {
        // Find the best next table: prefer one with equi-join predicate
        // connecting to current_schema, then smallest cardinality
        let best_idx = {
            let connected = remaining.iter().enumerate().find(|(_, (t, _))| {
                let t_schema = schema_of(t, ctx);
                remaining_preds.iter().any(|p| {
                    if p.operator != ComparisionOperator::EQ { return false; }
                    let ComparisionValue::Column(other) = &p.value else { return false; };
                    let lhs_in_current = current_schema.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_in_t       = t_schema.iter().any(|(n,_)| n == other.as_str());
                    let lhs_in_t       = t_schema.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_in_current = current_schema.iter().any(|(n,_)| n == other.as_str());
                    (lhs_in_current && rhs_in_t) || (lhs_in_t && rhs_in_current)
                })
            });
            if let Some((idx, _)) = connected { idx } else { 0 }
        };

        let (next_table, _) = remaining.remove(best_idx);
        let next_schema = schema_of(&next_table, ctx);

        // Apply literal predicates to next_table before joining
        // e.g. o_orderdate filters applied to orders before joining with customers
        let (next_filters, other): (Vec<_>, Vec<_>) =
            remaining_preds.into_iter().partition(|p| {
                let lhs_ok = next_schema.iter().any(|(n,_)| n == &p.column_name);
                !matches!(&p.value, ComparisionValue::Column(_)) && lhs_ok
            });

        let combined: Schema = current_schema.iter()
            .chain(next_schema.iter()).cloned().collect();

        // Extract join predicates applicable to current combined schema
        let (apply_now, apply_later): (Vec<_>, Vec<_>) =
            other.into_iter().partition(|p| {
                let lhs_ok = combined.iter().any(|(n,_)| n == &p.column_name);
                let rhs_ok = match &p.value {
                    ComparisionValue::Column(c) => combined.iter().any(|(n,_)| n == c),
                    _ => true,
                };
                lhs_ok && rhs_ok
            });

        remaining_preds = apply_later;

        // Wrap next_table with its literal filters before joining
        let filtered_next = if next_filters.is_empty() { next_table }
        else { QueryOp::Filter(FilterData {
            predicates: next_filters,
            underlying: Box::new(next_table),
        })};

        let cross = QueryOp::Cross(CrossData {
            left:  Box::new(current),
            right: Box::new(filtered_next),
        });

        current = if apply_now.is_empty() { cross }
        else { QueryOp::Filter(FilterData {
            predicates: apply_now,
            underlying: Box::new(cross),
        })};

        current_schema = combined;
    }

    if !remaining_preds.is_empty() {
        current = QueryOp::Filter(FilterData {
            predicates: remaining_preds,
            underlying: Box::new(current),
        });
    }

    current
}

fn collect_stacked_filters(op: QueryOp) -> (Vec<common::query::Predicate>, QueryOp) {
    match op {
        QueryOp::Filter(f) => {
            let (mut preds, underlying) = collect_stacked_filters(*f.underlying);
            preds.extend(f.predicates);
            (preds, underlying)
        }
        other => (vec![], other),
    }
}

fn reorder_joins(op: QueryOp, ctx: &DbContext) -> QueryOp {
    match op {
        QueryOp::Sort(s) => QueryOp::Sort(SortData {
            underlying: Box::new(reorder_joins(*s.underlying, ctx)),
            sort_specs: s.sort_specs,
        }),
        QueryOp::Project(p) => QueryOp::Project(ProjectData {
            underlying: Box::new(reorder_joins(*p.underlying, ctx)),
            column_name_map: p.column_name_map,
        }),
        QueryOp::Filter(f) => {
            // Collect ALL stacked filter predicates to see what's underneath
            let mut all_preds = f.predicates;
            let (more_preds, underlying) = collect_stacked_filters(*f.underlying);
            all_preds.extend(more_preds);

            if matches!(&underlying, QueryOp::Cross(_)) {
                let tables = flatten_cross_tree_owned(underlying);
                if tables.len() <= 2 {
                    let cross = tables.into_iter().reduce(|l, r| {
                        QueryOp::Cross(CrossData { left: Box::new(l), right: Box::new(r) })
                    }).unwrap();
                    return if all_preds.is_empty() { cross }
                    else { QueryOp::Filter(FilterData {
                        predicates: all_preds, underlying: Box::new(cross),
                    })};
                }
                // Use DP for 3-16 tables, greedy fallback for larger
                if tables.len() <= 16 {
                    return dp_optimize(tables, all_preds, ctx);
                }
                return build_incremental_join(tables, all_preds, ctx);
            }

            // Not a Cross underneath — just recurse
            let new_underlying = reorder_joins(underlying, ctx);
            if all_preds.is_empty() { new_underlying }
            else { QueryOp::Filter(FilterData {
                predicates: all_preds,
                underlying: Box::new(new_underlying),
            })}
        }
        other => other,
    }
}
fn push_project_past_sort(op: QueryOp, ctx: &DbContext) -> QueryOp {
    match op {
        QueryOp::Project(p) => {
            match *p.underlying {
                QueryOp::Sort(s) => {
                    let child_schema = schema_of(&s.underlying, ctx);

                    // Columns needed by outer project (source names)
                    let mut needed: Vec<String> = p.column_name_map.iter()
                        .map(|(from, _)| from.clone())
                        .collect();

                    // Columns needed by sort keys
                    for spec in &s.sort_specs {
                        if !needed.contains(&spec.column_name) {
                            needed.push(spec.column_name.clone());
                        }
                    }

                    // Only push down if it actually reduces column count
                    if needed.len() < child_schema.len() {
                        let early_map: Vec<(String, String)> = needed.iter()
                            .filter(|n| child_schema.iter().any(|(cn, _)| cn == *n))
                            .map(|n| (n.clone(), n.clone()))
                            .collect();

                        let early_proj = QueryOp::Project(ProjectData {
                            column_name_map: early_map,
                            underlying: Box::new(
                                push_project_past_sort(*s.underlying, ctx)
                            ),
                        });

                        let new_sort = QueryOp::Sort(SortData {
                            sort_specs: s.sort_specs,
                            underlying: Box::new(early_proj),
                        });

                        QueryOp::Project(ProjectData {
                            column_name_map: p.column_name_map,
                            underlying: Box::new(new_sort),
                        })
                    } else {
                        QueryOp::Project(ProjectData {
                            column_name_map: p.column_name_map,
                            underlying: Box::new(
                                push_project_past_sort(QueryOp::Sort(s), ctx)
                            ),
                        })
                    }
                }
                other => QueryOp::Project(ProjectData {
                    column_name_map: p.column_name_map,
                    underlying: Box::new(push_project_past_sort(other, ctx)),
                }),
            }
        }
        QueryOp::Sort(s) => QueryOp::Sort(SortData {
            sort_specs: s.sort_specs,
            underlying: Box::new(push_project_past_sort(*s.underlying, ctx)),
        }),
        QueryOp::Filter(f) => QueryOp::Filter(FilterData {
            predicates: f.predicates,
            underlying: Box::new(push_project_past_sort(*f.underlying, ctx)),
        }),
        QueryOp::Cross(c) => QueryOp::Cross(CrossData {
            left:  Box::new(push_project_past_sort(*c.left, ctx)),
            right: Box::new(push_project_past_sort(*c.right, ctx)),
        }),
        QueryOp::Scan(_) => op,
    }
}

fn optimize(op: QueryOp, ctx: &DbContext) -> QueryOp {
    let op = reorder_joins(op, ctx);
    let op = push_down_pass(op, ctx);
    // Step 3: push project inside sort — sort only materializes needed columns
    push_project_past_sort(op, ctx)
}

fn push_down_pass(op: QueryOp, ctx: &DbContext) -> QueryOp {
    match op {
        QueryOp::Filter(f) => {
            let c = push_down_pass(*f.underlying, ctx);
            push_filter_down(f.predicates, c, ctx)
        }
        QueryOp::Sort(s) => QueryOp::Sort(SortData {
            underlying: Box::new(push_down_pass(*s.underlying, ctx)),
            sort_specs: s.sort_specs,
        }),
        QueryOp::Project(p) => QueryOp::Project(ProjectData {
            underlying: Box::new(push_down_pass(*p.underlying, ctx)),
            column_name_map: p.column_name_map,
        }),
        QueryOp::Cross(c) => QueryOp::Cross(CrossData {
            left:  Box::new(push_down_pass(*c.left, ctx)),
            right: Box::new(push_down_pass(*c.right, ctx)),
        }),
        QueryOp::Scan(_) => op,
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    let (disk_in, mut disk_out)       = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();
    let mut disk_reader    = BufReader::new(disk_in);
    let mut monitor_reader = BufReader::new(monitor_in);

    // Receive query
    let mut line = String::new();
    monitor_reader.read_line(&mut line)?;
    let query: Query = serde_json::from_str(&line).context("Failed to parse query JSON")?;

    // Disk setup
    let block_size = disk_get_block_size(&mut disk_out, &mut disk_reader)?;
    let anon_start = disk_cmd_u64(&mut disk_out, &mut disk_reader, "get anon-start-block\n")?;
    let mut allocator = AnonAllocator::new(anon_start);

    // Memory limit
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    let mut mem_line = String::new();
    monitor_reader.read_line(&mut mem_line)?;

    // Optimize
    let query_root = optimize(query.root, &ctx);

    // Signal start — wrap monitor_out in BufWriter for batched row output
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    let mut buffered_monitor = BufWriter::with_capacity(256 * 1024, &mut monitor_out);

    execute(
        &query_root, &ctx,
        &mut disk_out, &mut disk_reader,
        block_size, &mut allocator,
        &mut |row, _| emit_row_to_monitor(&row, &mut buffered_monitor),
    )?;

    buffered_monitor.flush()?;
    drop(buffered_monitor);

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}

// Helper that appends serialized value into existing buffer to avoid many small Vec allocations
fn serialize_val_into_buf(v: &Data, buf: &mut Vec<u8>) {
    match v {
        Data::Int32(x)   => buf.extend_from_slice(&(*x as i64).to_le_bytes()),
        Data::Int64(x)   => buf.extend_from_slice(&x.to_le_bytes()),
        Data::Float32(x) => buf.extend_from_slice(&(*x as f64).to_le_bytes()),
        Data::Float64(x) => buf.extend_from_slice(&x.to_le_bytes()),
        Data::String(s)  => { buf.extend_from_slice(s.as_bytes()); buf.push(0); }
    }
}