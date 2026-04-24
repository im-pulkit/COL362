use common::query::{
    ComparisionOperator, ComparisionValue, MultiSortBuilder, MultiProjectBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("lineitem")
        .filter(
            "l_shipdate",
            ComparisionOperator::LTE,
            ComparisionValue::String(String::from("1998-09-02")),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_returnflag", true)
                .add("l_linestatus", true)
        )
        .project_multiple(
            MultiProjectBuilder::new("l_returnflag", "l_returnflag")
                .add("l_linestatus", "l_linestatus")
                .add("l_quantity", "l_quantity")
                .add("l_extendedprice", "l_extendedprice")
                .add("l_discount", "l_discount")
                .add("l_tax", "l_tax")
        )
        .build();

    println!("{}", serde_json::to_string_pretty(&query).unwrap());
}