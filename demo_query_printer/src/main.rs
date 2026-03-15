use common::query::{
    ComparisionOperator, ComparisionValue, MultiProjectBuilder, MultiSortBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("A")
        .cross(QueryOp::scan("B"))
        .filter(
            "a1",
            ComparisionOperator::EQ,
            ComparisionValue::Column(String::from("b1")),
        )
        .filter("b3", ComparisionOperator::GTE, ComparisionValue::I32(0))
        // You also have filter multiple
        .sort_multiple(MultiSortBuilder::new("a2", true).add("b2", false))
        .project_multiple(MultiProjectBuilder::new("a1", "id").add("b2", "b2"))
        .build();

    let query_json = serde_json::to_string_pretty(&query).unwrap();

    println!("{}", query_json);
}
