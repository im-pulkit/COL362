use common::query::{
    ComparisionOperator, ComparisionValue, MultiSortBuilder, MultiProjectBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("lineitem")
        .filter(
            "l_discount",
            ComparisionOperator::GT,
            ComparisionValue::F64(0.05),
        )
        .sort_multiple(MultiSortBuilder::new("l_extendedprice", false))
        .project_multiple(
            MultiProjectBuilder::new("l_orderkey", "l_orderkey")
                .add("l_extendedprice", "l_extendedprice")
        )
        .build();

    println!("{}", serde_json::to_string_pretty(&query).unwrap());
}