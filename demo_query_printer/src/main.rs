use common::query::{
    ComparisionOperator, ComparisionValue, MultiSortBuilder, MultiProjectBuilder, QueryOp,
};
fn main() {
    let query = QueryOp::scan("part")
        .cross(QueryOp::scan("supplier"))
        .cross(QueryOp::scan("partsupp"))
        .cross(QueryOp::scan("nation"))
        .cross(QueryOp::scan("region"))
        .filter("p_partkey", ComparisionOperator::EQ, ComparisionValue::Column("ps_partkey".into()))
        .filter("s_suppkey", ComparisionOperator::EQ, ComparisionValue::Column("ps_suppkey".into()))
        .filter("p_size", ComparisionOperator::EQ, ComparisionValue::I32(15))
        .filter("s_nationkey", ComparisionOperator::EQ, ComparisionValue::Column("n_nationkey".into()))
        .filter("n_regionkey", ComparisionOperator::EQ, ComparisionValue::Column("r_regionkey".into()))
        .filter("r_name", ComparisionOperator::EQ, ComparisionValue::String("EUROPE".into()))
        .sort_multiple(
            MultiSortBuilder::new("s_acctbal", false)
                .add("n_name", true)
                .add("s_name", true)
                .add("p_partkey", true)
                .add("ps_suppkey", true)
        )
        .project_multiple(
            MultiProjectBuilder::new("s_acctbal", "s_acctbal")
                .add("s_name", "s_name")
                .add("n_name", "n_name")
                .add("p_partkey", "p_partkey")
                .add("p_mfgr", "p_mfgr")
                .add("s_address", "s_address")
                .add("s_phone", "s_phone")
                .add("s_comment", "s_comment")
                .add("ps_supplycost", "ps_supplycost")
        )
        .build();

    println!("{}", serde_json::to_string_pretty(&query).unwrap());
}