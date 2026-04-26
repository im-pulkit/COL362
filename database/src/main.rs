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
const SORT_RUN_BYTES: usize      = 9 * 1024 * 1024; // 9MB — Sort run size

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
}

impl BlockWriter {
    fn new(block_size: u64, start: u64) -> Self {
        let bs = block_size as usize;
        Self { block_size: bs, usable: bs - 2, buf: vec![0u8; bs],
               offset: 0, row_count: 0, start_block: start, num_blocks: 0 }
    }

    fn push(&mut self, row: &Row, disk_out: &mut impl Write) -> Result<()> {
        let rb = serialize_row_bytes(row);
        assert!(rb.len() <= self.usable, "single row too large for block");
        if self.offset + rb.len() > self.usable { self.flush_block(disk_out)?; }
        self.buf[self.offset..self.offset + rb.len()].copy_from_slice(&rb);
        self.offset    += rb.len();
        self.row_count += 1;
        Ok(())
    }

    fn push_bytes(&mut self, rb: &[u8], disk_out: &mut impl Write) -> Result<()> {
        assert!(rb.len() <= self.usable, "single row too large for block");
        if self.offset + rb.len() > self.usable { self.flush_block(disk_out)?; }
        self.buf[self.offset..self.offset + rb.len()].copy_from_slice(rb);
        self.offset    += rb.len();
        self.row_count += 1;
        Ok(())
    }

    fn flush_block(&mut self, disk_out: &mut impl Write) -> Result<()> {
        if self.row_count == 0 { return Ok(()); }
        let cnt = self.row_count.to_le_bytes();
        self.buf[self.block_size - 2] = cnt[0];
        self.buf[self.block_size - 1] = cnt[1];
        let id = self.start_block + self.num_blocks;
        disk_out.write_all(format!("put block {} 1\n", id).as_bytes())?;
        disk_out.write_all(&self.buf)?;
        disk_out.flush()?;
        self.num_blocks += 1;
        self.buf        = vec![0u8; self.block_size];
        self.offset     = 0;
        self.row_count  = 0;
        Ok(())
    }

    fn finish(mut self, disk_out: &mut impl Write) -> Result<(u64, u64)> {
        self.flush_block(disk_out)?;
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
    p.column_name_map.iter()
        .map(|(from,to)| { let i=col_idx(schema,from); (to.clone(),schema[i].1.clone()) })
        .collect()
}

fn project_row(row: &Row, schema: &Schema, p: &ProjectData) -> Row {
    p.column_name_map.iter().map(|(from,_)| row[col_idx(schema,from)].clone()).collect()
}

fn apply_project_chain(row: Row, base: &Schema, projects: &[&ProjectData]) -> Row {
    if projects.is_empty() { return row; }
    let mut r=row; let mut s=base.clone();
    for p in projects { let nr=project_row(&r,&s,p); s=project_schema(&s,p); r=nr; }
    r
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
    let left = &row[col_idx(schema, &pred.column_name)];
    let right = match &pred.value {
        ComparisionValue::Column(c) => row[col_idx(schema, c)].clone(),
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

    for i in 0..num {
        let block=disk_read_block(disk_out,disk_in,start+i,block_size)?;
        for row in parse_block(&block,&col_types) {
            // Apply pre-projection filters against raw schema
            if !passes_all_filters(&row,&base_schema,&pre_filters) { continue; }

            let final_row=apply_project_chain(row,&base_schema,&pipeline.projects);

            // Apply post-projection filters against output schema
            if !passes_all_filters(&final_row,&out_schema,&post_filters) { continue; }

            emit(final_row,&out_schema)?;
        }
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
    fn load_next_block(
        &mut self,
        disk_out:   &mut impl Write,
        disk_in:    &mut (impl BufRead + Read),
        block_size: u64,
    ) -> Result<bool> {
        if self.cur_block >= self.num_blocks { return Ok(false); }
        let block = disk_read_block(disk_out, disk_in,
            self.start_block + self.cur_block, block_size)?;
        self.cur_block += 1;
        self.rows    = parse_block(&block, &self.col_types);
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


// ── Cross (block nested loop join with in-memory fast path) ───────────────────
//
// Key optimization: if right side fits in MEMORY_BUDGET_BYTES, keep it in RAM.
// No anon disk writes → no seeks to the 19M+ block region → zero cylinder travel.
//
// Bonus optimization: join predicates (those referencing both sides) are applied
// INSIDE the cross loop, not outside. This means we only emit pairs that pass,
// avoiding massive tuple overhead for large cross products.


/// Estimate number of blocks for an op subtree using stats or file size
// fn estimate_blocks(op: &QueryOp, ctx: &DbContext) -> u64 {
//     match op {
//         QueryOp::Scan(s) => {
//             // Use CardinalityStat if available, else return a large number
//             let table = ctx.get_table_specs().iter()
//                 .find(|t| t.name == s.table_id);
//             if let Some(t) = table {
//                 // Use cardinality of first column with CardinalityStat
//                 for col in &t.column_specs {
//                     if let Some(stats) = &col.stats {
//                         for stat in stats {
//                             if let db_config::statistics::ColumnStat::CardinalityStat(c) = stat {
//                                 eprintln!("[DEBUG] CardinalityStat for {} = {}", s.table_id, c.0);
//                                 return c.0; // row count estimate
//                             }
//                         }
//                     }
//                 }
//             }
//             eprintln!("[DEBUG] no CardinalityStat for {}", s.table_id);
//             u64::MAX // unknown — assume large
//         }
//         QueryOp::Filter(f) => {
//             // After filter, assume 10% selectivity if no stats
//             estimate_blocks(&f.underlying, ctx) / 10
//         }
//         QueryOp::Project(p) => estimate_blocks(&p.underlying, ctx),
//         QueryOp::Sort(s)    => estimate_blocks(&s.underlying, ctx),
//         QueryOp::Cross(c)   => {
//             let l = estimate_blocks(&c.left, ctx);
//             let r = estimate_blocks(&c.right, ctx);
//             l.saturating_mul(r)
//         }
//     }
// }
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
        for i in 0..right_num {
            right_blocks.push(disk_read_block(disk_out, disk_in, right_start + i, block_size)?);
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
    for i in 0..file_num {
        let block = disk_read_block(disk_out, disk_in, file_start + i, block_size)?;
        for row in parse_block(&block, &col_types) {
            if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
            let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
            if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }
            writer.push(&final_row, disk_out)?;
        }
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

const NUM_PARTITIONS: usize = 64;

fn hash_key(key: &[String]) -> usize {
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
        let mut hash_table: HashMap<Vec<String>, Vec<Row>> = HashMap::new();
        for i in 0..rn {
            let block = disk_read_block(disk_out, disk_in, rs + i, block_size)?;
            for row in parse_block(&block, &right_col_types) {
                let key = extract_key(&row, &right_schema, &equi_pairs, false);
                hash_table.entry(key).or_default().push(row);
            }
        }

        // Probe with left partition
        for i in 0..ln {
            let block = disk_read_block(disk_out, disk_in, ls + i, block_size)?;
            for left_row in parse_block(&block, &left_col_types) {
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
        let mut hash_table: HashMap<Vec<String>, Vec<Row>> = HashMap::new();
        for i in 0..rn {
            let block = disk_read_block(disk_out, disk_in, rs + i, block_size)?;
            for row in parse_block(&block, &right_col_types) {
                let key = extract_key(&row, &right_schema, &equi_pairs, false);
                hash_table.entry(key).or_default().push(row);
            }
        }

        for i in 0..ln {
            let block = disk_read_block(disk_out, disk_in, ls + i, block_size)?;
            for left_row in parse_block(&block, &left_col_types) {
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
) -> (Schema, Schema, Schema, Vec<(String, String)>, Vec<&'a common::query::Predicate>) {
    let left_schema  = schema_of(&cross_data.left, ctx);
    let right_schema = schema_of(&cross_data.right, ctx);

    let mut equi_pairs: Vec<(String, String)> = Vec::new();
    let mut theta: Vec<&common::query::Predicate> = Vec::new();

    for p in join_predicates {
        if p.operator == ComparisionOperator::EQ {
            if let ComparisionValue::Column(other) = &p.value {
                let lhs_left  = left_schema.iter().any(|(n,_)| n == &p.column_name);
                let rhs_right = right_schema.iter().any(|(n,_)| n == other.as_str());
                let lhs_right = right_schema.iter().any(|(n,_)| n == &p.column_name);
                let rhs_left  = left_schema.iter().any(|(n,_)| n == other.as_str());
                if lhs_left && rhs_right {
                    equi_pairs.push((p.column_name.clone(), other.clone()));
                    continue;
                } else if lhs_right && rhs_left {
                    equi_pairs.push((other.clone(), p.column_name.clone()));
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
    schema:     &Schema,
    equi_pairs: &[(String, String)],
    is_left:    bool,
) -> Vec<String> {
    equi_pairs.iter().map(|(lc, rc)| {
        let col = if is_left { lc } else { rc };
        let idx = schema.iter().position(|(n,_)| n == col)
            .unwrap_or_else(|| panic!("col {} not in schema", col));
        format_value(&row[idx])
    }).collect()
}

fn partition_both_sides(
    cross_data:   &CrossData,
    equi_pairs:   &[(String, String)],
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
    let left_partition_starts: Vec<u64> = (0..NUM_PARTITIONS)
        .map(|_| { let s = allocator.next; allocator.next += 4096; s })
        .collect();
    let right_partition_starts: Vec<u64> = (0..NUM_PARTITIONS)
        .map(|_| { let s = allocator.next; allocator.next += 4096; s })
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
    equi_pairs:  &[(String, String)],
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

    for i in 0..spill_num {
        let block = disk_read_block(disk_out, disk_in, spill_start + i, block_size)?;
        for row in parse_block(&block, &col_types) {
            let key = extract_key(&row, &spill_schema, equi_pairs, is_left);
            let p = hash_key(&key);
            writers[p].push(&row, disk_out)?;
        }
    }
    Ok(())
}

// Extracted helper for Cross(pipeline, pipeline) case
fn partition_cross_pipelines(
    left_pipe:  &Pipeline,
    right_pipe: &Pipeline,
    equi_pairs: &[(String, String)],
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
    for i in 0..rn {
        let block = disk_read_block(disk_out, disk_in, rs + i, block_size)?;
        for row in parse_block(&block, &right_types) {
            if !passes_all_filters(&row, &right_base, &right_pipe.filters) { continue; }
            right_rows.push(apply_project_chain(row, &right_base, &right_pipe.projects));
        }
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

    for i in 0..ln {
        let block = disk_read_block(disk_out, disk_in, ls + i, block_size)?;
        for left_row in parse_block(&block, &left_types) {
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
    Ok(())
}

fn partition_pipeline(
    pipeline:    &Pipeline,
    equi_pairs:  &[(String, String)],
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

    for i in 0..num {
        let block = disk_read_block(disk_out, disk_in, start + i, block_size)?;
        for row in parse_block(&block, &col_types) {
            if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
            let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
            if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }
            let key = extract_key(&final_row, &out_schema, equi_pairs, is_left);
            let p = hash_key(&key);
            // Write to partition writer — disk_out used sequentially, no borrow conflict
            writers[p].push(&final_row, disk_out)?;
        }
    }
    Ok(())
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
    if let QueryOp::Filter(f) = op {
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

            return spill_grace_hash_join_to_anon(
                c, &f.predicates,
                ctx, disk_out, disk_in, block_size, allocator,
            );
        }
    }
    if let QueryOp::Cross(c) = op {
        return spill_grace_hash_join_to_anon(
            c, &[], ctx, disk_out, disk_in, block_size, allocator,
        );
    }
    // Fallback for small results
    // eprintln!("[SPILL_GENERIC] collecting rows for fallback...");
    let mut rows: Vec<Row> = Vec::new();
    let mut schema_out: Option<Schema> = None;
    let child_schema = execute(op, ctx, disk_out, disk_in, block_size, allocator,
        &mut |row, schema| {
            if schema_out.is_none() { schema_out = Some(schema.clone()); }
            rows.push(row);
            Ok(())
        })?;
    // eprintln!("[SPILL_GENERIC] collected {} rows", rows.len());
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
        // eprintln!("[SORT] using pipeline path for {}", pipeline.scan.table_id);
        return exec_sort_from_pipeline(
            &pipeline, sort_data, ctx, disk_out, disk_in, block_size, allocator, emit
        );
    }
    // eprintln!("[SORT] using slow path");

    let (child_start, child_num, child_schema) =
        spill_hash_join_to_anon_generic(
            &sort_data.underlying, ctx, disk_out, disk_in, block_size, allocator
        )?;
    // eprintln!("[SORT] child_num={} blocks", child_num);

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
            let block = disk_read_block(disk_out, disk_in,
                child_start + block_idx + blocks_read, block_size)?;
            let new_rows = parse_block(&block, &col_types);
            blocks_read += 1;

            for row in new_rows {
                batch_bytes += approx_row_size(&row);
                batch.push(row);
            }

            if batch_bytes >= SORT_RUN_BYTES || batch.len() >= 5000 {
                break;
            }
        }
        // eprintln!("[SORT] batch: {} rows, {} bytes", batch.len(), batch_bytes);

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
        for i in 0..rn {
            let block = disk_read_block(disk_out, disk_in, rs + i, block_size)?;
            for row in parse_block(&block, &col_types) {
                emit(row, &child_schema)?;
            }
        }
    } else {
        // eprintln!("[SORT] {} runs to merge", runs.len());
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

    for i in 0..num {
        let block = disk_read_block(disk_out, disk_in, start + i, block_size)?;
        for row in parse_block(&block, &col_types) {
            if !passes_all_filters(&row, &base_schema, &pre_filters) { continue; }
            let final_row = apply_project_chain(row, &base_schema, &pipeline.projects);
            if !passes_all_filters(&final_row, &out_schema, &post_filters) { continue; }

            // // Debug first row
            // if total_rows == 0 {
            //     eprintln!("[SORT_DEBUG] first row size estimate: {}, cols: {}",
            //         serialize_row_bytes(&final_row).len(), final_row.len());
            //     eprintln!("[SORT_DEBUG] SORT_RUN_BYTES={}", SORT_RUN_BYTES);
            // }

            batch_bytes += serialize_row_bytes(&final_row).len();
            batch.push(final_row);
            total_rows += 1;

            if batch_bytes > SORT_RUN_BYTES {
                // eprintln!("[SORT_DEBUG] spilling run at row {}, batch_bytes={}",
                //     total_rows, batch_bytes);
                batch.sort_by(|a, b|
                    compare_rows(a, b, &out_schema, &sort_data.sort_specs));
                let (rs, rn) = spill_rows_to_anon(&batch, disk_out, block_size, allocator)?;
                if rn > 0 { runs.push((rs, rn)); }
                batch.clear();
                batch_bytes = 0;
            }
        }
    }

    // eprintln!("[SORT_DEBUG] total_rows={} runs={}", total_rows, runs.len());

    if runs.is_empty() {
        batch.sort_by(|a, b|
            compare_rows(a, b, &out_schema, &sort_data.sort_specs));
        for row in batch { emit(row, &out_schema)?; }
        return Ok(out_schema);
    }

    if !batch.is_empty() {
        batch.sort_by(|a, b|
            compare_rows(a, b, &out_schema, &sort_data.sort_specs));
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
    let col_types: Vec<DataType> = schema.iter().map(|(_, t)| t.clone()).collect();
    let mut states: Vec<RunState> = Vec::with_capacity(runs.len());

    for (rs, rn) in &runs {
        if *rn == 0 { continue; }
        let block = disk_read_block(disk_out, disk_in, *rs, block_size)?;
        let rows  = parse_block(&block, &col_types);
        states.push(RunState {
            start_block: *rs, num_blocks: *rn,
            cur_block: 1, rows, row_idx: 0,
        });
    }

    loop {
        let mut min_idx: Option<usize> = None;
        for (i, state) in states.iter().enumerate() {
            if state.peek().is_none() { continue; }
            match min_idx {
                None    => min_idx = Some(i),
                Some(j) => {
                    if compare_rows(
                        state.peek().unwrap(), states[j].peek().unwrap(),
                        schema, &sort_data.sort_specs,
                    ) == std::cmp::Ordering::Less {
                        min_idx = Some(i);
                    }
                }
            }
        }
        let Some(idx) = min_idx else { break; };
        let row = states[idx].peek().unwrap().clone();
        emit(row, schema)?;
        states[idx].advance(disk_out, disk_in, block_size, &col_types)?;
    }
    Ok(())
}


struct RunState {
    start_block: u64, num_blocks: u64, cur_block: u64,
    rows: Vec<Row>, row_idx: usize,
}
impl RunState {
    fn peek(&self) -> Option<&Row> {
        if self.row_idx < self.rows.len() { Some(&self.rows[self.row_idx]) } else { None }
    }
    fn advance(&mut self, disk_out: &mut impl Write, disk_in: &mut (impl BufRead+Read),
               block_size: u64, col_types: &[DataType]) -> Result<()> {
        self.row_idx+=1;
        if self.row_idx>=self.rows.len() && self.cur_block<self.num_blocks {
            let block=disk_read_block(disk_out,disk_in,self.start_block+self.cur_block,block_size)?;
            self.rows=parse_block(&block,col_types);
            self.cur_block+=1; self.row_idx=0;
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

/// Flatten a nested Cross tree into a list of leaf ops
// fn flatten_cross_tree(op: &QueryOp) -> Vec<&QueryOp> {
//     match op {
//         QueryOp::Cross(c) => {
//             let mut left = flatten_cross_tree(&c.left);
//             left.extend(flatten_cross_tree(&c.right));
//             left
//         }
//         other => vec![other],
//     }
// }

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
    let mut current = first;
    let mut current_schema = schema_of(&current, ctx);

    while !remaining.is_empty() {
        // Find the best next table: prefer one with equi-join predicate
        // connecting to current_schema, then smallest cardinality
        let best_idx = {
            // First try: find table with equi-join predicate to current schema
            let connected = remaining.iter().enumerate().find(|(_, (t, _))| {
                let t_schema = schema_of(t, ctx);
                remaining_preds.iter().any(|p| {
                    if p.operator != ComparisionOperator::EQ { return false; }
                    let ComparisionValue::Column(other) = &p.value else { return false; };
                    let lhs_in_current  = current_schema.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_in_t        = t_schema.iter().any(|(n,_)| n == other.as_str());
                    let lhs_in_t        = t_schema.iter().any(|(n,_)| n == &p.column_name);
                    let rhs_in_current  = current_schema.iter().any(|(n,_)| n == other.as_str());
                    (lhs_in_current && rhs_in_t) || (lhs_in_t && rhs_in_current)
                })
            });

            if let Some((idx, _)) = connected {
                idx
            } else {
                // No connected table — pick smallest (cartesian product, unavoidable)
                0 // already sorted by cardinality
            }
        };

        let (next_table, _) = remaining.remove(best_idx);
        let next_schema = schema_of(&next_table, ctx);
        let combined: Schema = current_schema.iter()
            .chain(next_schema.iter()).cloned().collect();

        // Extract predicates where ALL referenced columns are now available
        let (apply_now, apply_later): (Vec<_>, Vec<_>) =
            remaining_preds.into_iter().partition(|p| {
                let lhs_ok = combined.iter().any(|(n,_)| n == &p.column_name);
                let rhs_ok = match &p.value {
                    ComparisionValue::Column(c) => combined.iter().any(|(n,_)| n == c),
                    _ => true,
                };
                lhs_ok && rhs_ok
            });

        remaining_preds = apply_later;

        let cross = QueryOp::Cross(CrossData {
            left:  Box::new(current),
            right: Box::new(next_table),
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

fn optimize(op: QueryOp, ctx: &DbContext) -> QueryOp {
    // Step 1: reorder joins on raw tree BEFORE pushing predicates down
    let op = reorder_joins(op, ctx);
    // Step 2: push predicates down on reordered tree
    push_down_pass(op, ctx)
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