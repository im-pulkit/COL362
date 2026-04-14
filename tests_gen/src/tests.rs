use common::query::{
    ComparisionOperator, ComparisionValue, MultiPredicateBuilder, MultiProjectBuilder,
    MultiSortBuilder, Query, QueryOp,
};

// ============================================================================
// CATEGORY 1: SCAN + PROJECT (Base Cases)
// Testing basic table scans with specific column projections.
// ============================================================================

/*
SELECT n_nationkey, n_name, n_regionkey, '' FROM nation;
*/
pub fn test_q1() -> (Query, String, bool) {
    let sql = "SELECT n_nationkey, n_name, n_regionkey, '' FROM nation;";
    let query = QueryOp::scan("nation")
        .project_multiple(
            MultiProjectBuilder::new("n_nationkey", "n_nationkey")
                .add("n_name", "n_name")
                .add("n_regionkey", "n_regionkey"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT r_regionkey, r_name, '' FROM region;
*/
pub fn test_q2() -> (Query, String, bool) {
    let sql = "SELECT r_regionkey, r_name, '' FROM region;";
    let query = QueryOp::scan("region")
        .project_multiple(
            MultiProjectBuilder::new("r_regionkey", "r_regionkey").add("r_name", "r_name"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, '' FROM part;
*/
pub fn test_q3() -> (Query, String, bool) {
    let sql = "SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, '' FROM part;";
    let query = QueryOp::scan("part")
        .project_multiple(
            MultiProjectBuilder::new("p_partkey", "p_partkey")
                .add("p_name", "p_name")
                .add("p_mfgr", "p_mfgr")
                .add("p_brand", "p_brand")
                .add("p_type", "p_type"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT s_suppkey, s_name, s_acctbal, '' FROM supplier;
*/
pub fn test_q4() -> (Query, String, bool) {
    let sql = "SELECT s_suppkey, s_name, s_acctbal, '' FROM supplier;";
    let query = QueryOp::scan("supplier")
        .project_multiple(
            MultiProjectBuilder::new("s_suppkey", "s_suppkey")
                .add("s_name", "s_name")
                .add("s_acctbal", "s_acctbal"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT c_custkey, c_name, c_mktsegment, '' FROM customer;
*/
pub fn test_q5() -> (Query, String, bool) {
    let sql = "SELECT c_custkey, c_name, c_mktsegment, '' FROM customer;";
    let query = QueryOp::scan("customer")
        .project_multiple(
            MultiProjectBuilder::new("c_custkey", "c_custkey")
                .add("c_name", "c_name")
                .add("c_mktsegment", "c_mktsegment"),
        )
        .build();
    (query, sql.to_string(), false)
}

// ============================================================================
// CATEGORY 2: SCAN + FILTER + PROJECT
// Testing point lookups, inequalities, and various data types.
// ============================================================================

/*
SELECT n_name, '' FROM nation WHERE n_regionkey = 1;
*/
pub fn test_q6() -> (Query, String, bool) {
    let sql = "SELECT n_name, '' FROM nation WHERE n_regionkey = 1;";
    let query = QueryOp::scan("nation")
        .filter(
            "n_regionkey",
            ComparisionOperator::EQ,
            ComparisionValue::I32(1),
        )
        .project("n_name", "n_name")
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT r_name, '' FROM region WHERE r_name != 'AFRICA';
*/
pub fn test_q7() -> (Query, String, bool) {
    let sql = "SELECT r_name, '' FROM region WHERE r_name != 'AFRICA';";
    let query = QueryOp::scan("region")
        .filter(
            "r_name",
            ComparisionOperator::NE,
            ComparisionValue::String("AFRICA".to_string()),
        )
        .project("r_name", "r_name")
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT p_name, p_size, '' FROM part WHERE p_size > 25;
*/
pub fn test_q8() -> (Query, String, bool) {
    let sql = "SELECT p_name, p_size, '' FROM part WHERE p_size > 25;";
    let query = QueryOp::scan("part")
        .filter("p_size", ComparisionOperator::GT, ComparisionValue::I32(25))
        .project_multiple(MultiProjectBuilder::new("p_name", "p_name").add("p_size", "p_size"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT p_partkey, p_retailprice, '' FROM part WHERE p_retailprice >= 1000.00;
*/
pub fn test_q9() -> (Query, String, bool) {
    let sql = "SELECT p_partkey, p_retailprice, '' FROM part WHERE p_retailprice >= 1000.00;";
    let query = QueryOp::scan("part")
        .filter(
            "p_retailprice",
            ComparisionOperator::GTE,
            ComparisionValue::F64(1000.00),
        )
        .project_multiple(
            MultiProjectBuilder::new("p_partkey", "p_partkey")
                .add("p_retailprice", "p_retailprice"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT s_name, s_acctbal, '' FROM supplier WHERE s_acctbal < 0.0;
*/
pub fn test_q10() -> (Query, String, bool) {
    let sql = "SELECT s_name, s_acctbal, '' FROM supplier WHERE s_acctbal < 0.0;";
    let query = QueryOp::scan("supplier")
        .filter(
            "s_acctbal",
            ComparisionOperator::LT,
            ComparisionValue::F64(0.0),
        )
        .project_multiple(
            MultiProjectBuilder::new("s_name", "s_name").add("s_acctbal", "s_acctbal"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT c_name, c_mktsegment, '' FROM customer WHERE c_mktsegment = 'AUTOMOBILE';
*/
pub fn test_q11() -> (Query, String, bool) {
    let sql = "SELECT c_name, c_mktsegment, '' FROM customer WHERE c_mktsegment = 'AUTOMOBILE';";
    let query = QueryOp::scan("customer")
        .filter(
            "c_mktsegment",
            ComparisionOperator::EQ,
            ComparisionValue::String("AUTOMOBILE".to_string()),
        )
        .project_multiple(
            MultiProjectBuilder::new("c_name", "c_name").add("c_mktsegment", "c_mktsegment"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT o_orderkey, o_totalprice, '' FROM orders WHERE o_orderstatus = 'O';
*/
pub fn test_q12() -> (Query, String, bool) {
    let sql = "SELECT o_orderkey, o_totalprice, '' FROM orders WHERE o_orderstatus = 'O';";
    let query = QueryOp::scan("orders")
        .filter(
            "o_orderstatus",
            ComparisionOperator::EQ,
            ComparisionValue::String("O".to_string()),
        )
        .project_multiple(
            MultiProjectBuilder::new("o_orderkey", "o_orderkey")
                .add("o_totalprice", "o_totalprice"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT l_orderkey, l_linenumber, '' FROM lineitem WHERE l_quantity <= 10;
*/
pub fn test_q13() -> (Query, String, bool) {
    let sql = "SELECT l_orderkey, l_linenumber, '' FROM lineitem WHERE l_quantity <= 10;";
    let query = QueryOp::scan("lineitem")
        .filter(
            "l_quantity",
            ComparisionOperator::LTE,
            ComparisionValue::I32(10),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_orderkey", "l_orderkey")
                .add("l_linenumber", "l_linenumber"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT ps_partkey, ps_suppkey, '' FROM partsupp WHERE ps_availqty > 5000;
*/
pub fn test_q14() -> (Query, String, bool) {
    let sql = "SELECT ps_partkey, ps_suppkey, '' FROM partsupp WHERE ps_availqty > 5000;";
    let query = QueryOp::scan("partsupp")
        .filter(
            "ps_availqty",
            ComparisionOperator::GT,
            ComparisionValue::I32(5000),
        )
        .project_multiple(
            MultiProjectBuilder::new("ps_partkey", "ps_partkey").add("ps_suppkey", "ps_suppkey"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT l_orderkey, l_discount, '' FROM lineitem WHERE l_discount >= 0.05 AND l_discount <= 0.07;
*/
pub fn test_q15() -> (Query, String, bool) {
    let sql = "SELECT l_orderkey, l_discount, '' FROM lineitem WHERE l_discount >= 0.05 AND l_discount <= 0.07;";
    let query = QueryOp::scan("lineitem")
        .filter_multiple(
            MultiPredicateBuilder::new(
                "l_discount",
                ComparisionOperator::GTE,
                ComparisionValue::F64(0.05),
            )
            .add(
                "l_discount",
                ComparisionOperator::LTE,
                ComparisionValue::F64(0.07),
            ),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_orderkey", "l_orderkey").add("l_discount", "l_discount"),
        )
        .build();
    (query, sql.to_string(), false)
}

// ============================================================================
// CATEGORY 3: SCAN + SORT + PROJECT
// Testing pure ordering with deterministic tie-breakers (Primary Keys).
// ============================================================================

/*
SELECT n_name, '' FROM nation ORDER BY n_name, n_nationkey;
*/
pub fn test_q16() -> (Query, String, bool) {
    let sql = "SELECT n_name, '' FROM nation ORDER BY n_name, n_nationkey;";
    let query = QueryOp::scan("nation")
        .sort_multiple(MultiSortBuilder::new("n_name", true).add("n_nationkey", true))
        .project("n_name", "n_name")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT r_name, '' FROM region ORDER BY r_name DESC, r_regionkey;
*/
pub fn test_q17() -> (Query, String, bool) {
    let sql = "SELECT r_name, '' FROM region ORDER BY r_name DESC, r_regionkey;";
    let query = QueryOp::scan("region")
        .sort_multiple(MultiSortBuilder::new("r_name", false).add("r_regionkey", true))
        .project("r_name", "r_name")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT p_brand, p_size, '' FROM part ORDER BY p_brand, p_size DESC, p_partkey;
*/
pub fn test_q18() -> (Query, String, bool) {
    let sql = "SELECT p_brand, p_size, '' FROM part ORDER BY p_brand, p_size DESC, p_partkey;";
    let query = QueryOp::scan("part")
        .sort_multiple(
            MultiSortBuilder::new("p_brand", true)
                .add("p_size", false)
                .add("p_partkey", true),
        )
        .project_multiple(MultiProjectBuilder::new("p_brand", "p_brand").add("p_size", "p_size"))
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT s_acctbal, s_name, '' FROM supplier ORDER BY s_acctbal DESC, s_suppkey;
*/
pub fn test_q19() -> (Query, String, bool) {
    let sql = "SELECT s_acctbal, s_name, '' FROM supplier ORDER BY s_acctbal DESC, s_suppkey;";
    let query = QueryOp::scan("supplier")
        .sort_multiple(MultiSortBuilder::new("s_acctbal", false).add("s_suppkey", true))
        .project_multiple(
            MultiProjectBuilder::new("s_acctbal", "s_acctbal").add("s_name", "s_name"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT c_mktsegment, c_acctbal, '' FROM customer ORDER BY c_mktsegment, c_acctbal DESC, c_custkey;
*/
pub fn test_q20() -> (Query, String, bool) {
    let sql = "SELECT c_mktsegment, c_acctbal, '' FROM customer ORDER BY c_mktsegment, c_acctbal DESC, c_custkey;";
    let query = QueryOp::scan("customer")
        .sort_multiple(
            MultiSortBuilder::new("c_mktsegment", true)
                .add("c_acctbal", false)
                .add("c_custkey", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("c_mktsegment", "c_mktsegment").add("c_acctbal", "c_acctbal"),
        )
        .build();
    (query, sql.to_string(), true)
}

// ============================================================================
// CATEGORY 4: SCAN + FILTER + SORT + PROJECT
// Testing pipelines of data restriction followed by sorting.
// ============================================================================

/*
SELECT n_name, '' FROM nation WHERE n_regionkey = 3 ORDER BY n_name DESC, n_nationkey;
*/
pub fn test_q21() -> (Query, String, bool) {
    let sql = "SELECT n_name, '' FROM nation WHERE n_regionkey = 3 ORDER BY n_name DESC, n_nationkey;";
    let query = QueryOp::scan("nation")
        .filter(
            "n_regionkey",
            ComparisionOperator::EQ,
            ComparisionValue::I32(3),
        )
        .sort_multiple(MultiSortBuilder::new("n_name", false).add("n_nationkey", true))
        .project("n_name", "n_name")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT p_name, '' FROM part WHERE p_size < 10 ORDER BY p_name, p_partkey;
*/
pub fn test_q22() -> (Query, String, bool) {
    let sql = "SELECT p_name, '' FROM part WHERE p_size < 10 ORDER BY p_name, p_partkey;";
    let query = QueryOp::scan("part")
        .filter("p_size", ComparisionOperator::LT, ComparisionValue::I32(10))
        .sort_multiple(MultiSortBuilder::new("p_name", true).add("p_partkey", true))
        .project("p_name", "p_name")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT s_name, s_acctbal, '' FROM supplier WHERE s_acctbal > 5000.0 ORDER BY s_acctbal DESC, s_suppkey;
*/
pub fn test_q23() -> (Query, String, bool) {
    let sql = "SELECT s_name, s_acctbal, '' FROM supplier WHERE s_acctbal > 5000.0 ORDER BY s_acctbal DESC, s_suppkey;";
    let query = QueryOp::scan("supplier")
        .filter(
            "s_acctbal",
            ComparisionOperator::GT,
            ComparisionValue::F64(5000.0),
        )
        .sort_multiple(MultiSortBuilder::new("s_acctbal", false).add("s_suppkey", true))
        .project_multiple(
            MultiProjectBuilder::new("s_name", "s_name").add("s_acctbal", "s_acctbal"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT c_name, '' FROM customer WHERE c_mktsegment = 'HOUSEHOLD' ORDER BY c_name, c_custkey;
*/
pub fn test_q24() -> (Query, String, bool) {
    let sql = "SELECT c_name, '' FROM customer WHERE c_mktsegment = 'HOUSEHOLD' ORDER BY c_name, c_custkey;";
    let query = QueryOp::scan("customer")
        .filter(
            "c_mktsegment",
            ComparisionOperator::EQ,
            ComparisionValue::String("HOUSEHOLD".to_string()),
        )
        .sort_multiple(MultiSortBuilder::new("c_name", true).add("c_custkey", true))
        .project("c_name", "c_name")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT o_orderkey, o_totalprice, '' FROM orders WHERE o_orderstatus = 'F' ORDER BY o_totalprice DESC, o_orderkey;
*/
pub fn test_q25() -> (Query, String, bool) {
    let sql = "SELECT o_orderkey, o_totalprice, '' FROM orders WHERE o_orderstatus = 'F' ORDER BY o_totalprice DESC, o_orderkey;";
    let query = QueryOp::scan("orders")
        .filter(
            "o_orderstatus",
            ComparisionOperator::EQ,
            ComparisionValue::String("F".to_string()),
        )
        .sort_multiple(MultiSortBuilder::new("o_totalprice", false).add("o_orderkey", true))
        .project_multiple(
            MultiProjectBuilder::new("o_orderkey", "o_orderkey")
                .add("o_totalprice", "o_totalprice"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT l_orderkey, l_quantity, '' FROM lineitem WHERE l_shipdate > '1998-01-01' ORDER BY l_quantity DESC, l_orderkey, l_linenumber;
*/
pub fn test_q26() -> (Query, String, bool) {
    let sql = "SELECT l_orderkey, l_quantity, '' FROM lineitem WHERE l_shipdate > '1998-01-01' ORDER BY l_quantity DESC, l_orderkey, l_linenumber;";
    let query = QueryOp::scan("lineitem")
        .filter(
            "l_shipdate",
            ComparisionOperator::GT,
            ComparisionValue::String("1998-01-01".to_string()),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_quantity", false)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_orderkey", "l_orderkey").add("l_quantity", "l_quantity"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT ps_suppkey, '' FROM partsupp WHERE ps_availqty < 100 ORDER BY ps_suppkey, ps_partkey;
*/
pub fn test_q27() -> (Query, String, bool) {
    let sql = "SELECT ps_suppkey, '' FROM partsupp WHERE ps_availqty < 100 ORDER BY ps_suppkey, ps_partkey;";
    let query = QueryOp::scan("partsupp")
        .filter(
            "ps_availqty",
            ComparisionOperator::LT,
            ComparisionValue::I32(100),
        )
        .sort_multiple(MultiSortBuilder::new("ps_suppkey", true).add("ps_partkey", true))
        .project("ps_suppkey", "ps_suppkey")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT l_extendedprice, '' FROM lineitem WHERE l_returnflag = 'R' AND l_linestatus = 'F' ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber;
*/
pub fn test_q28() -> (Query, String, bool) {
    let sql = "SELECT l_extendedprice, '' FROM lineitem WHERE l_returnflag = 'R' AND l_linestatus = 'F' ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber;";
    let query = QueryOp::scan("lineitem")
        .filter_multiple(
            MultiPredicateBuilder::new(
                "l_returnflag",
                ComparisionOperator::EQ,
                ComparisionValue::String("R".to_string()),
            )
            .add(
                "l_linestatus",
                ComparisionOperator::EQ,
                ComparisionValue::String("F".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_extendedprice", false)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project("l_extendedprice", "l_extendedprice")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT c_phone, '' FROM customer WHERE c_acctbal < 0.0 ORDER BY c_phone, c_custkey;
*/
pub fn test_q29() -> (Query, String, bool) {
    let sql = "SELECT c_phone, '' FROM customer WHERE c_acctbal < 0.0 ORDER BY c_phone, c_custkey;";
    let query = QueryOp::scan("customer")
        .filter(
            "c_acctbal",
            ComparisionOperator::LT,
            ComparisionValue::F64(0.0),
        )
        .sort_multiple(MultiSortBuilder::new("c_phone", true).add("c_custkey", true))
        .project("c_phone", "c_phone")
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT p_mfgr, '' FROM part WHERE p_type = 'BRASS' ORDER BY p_mfgr, p_partkey;
*/
pub fn test_q30() -> (Query, String, bool) {
    let sql = "SELECT p_mfgr, '' FROM part WHERE p_type = 'BRASS' ORDER BY p_mfgr, p_partkey;";
    let query = QueryOp::scan("part")
        .filter(
            "p_type",
            ComparisionOperator::EQ,
            ComparisionValue::String("BRASS".to_string()),
        )
        .sort_multiple(MultiSortBuilder::new("p_mfgr", true).add("p_partkey", true))
        .project("p_mfgr", "p_mfgr")
        .build();
    (query, sql.to_string(), true)
}

// ============================================================================
// CATEGORY 5: SCAN + CROSS + PROJECT
// Testing Cartesian products (Cross joins) without filters.
// ============================================================================
/*
SELECT n_name, r_name, '' FROM nation, region;
*/
pub fn test_q31() -> (Query, String, bool) {
    let sql = "SELECT n_name, r_name, '' FROM nation, region;";
    let query = QueryOp::scan("nation")
        .cross(QueryOp::scan("region"))
        .project_multiple(MultiProjectBuilder::new("n_name", "n_name").add("r_name", "r_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT s_name, r_name, '' FROM supplier, region;
*/
pub fn test_q32() -> (Query, String, bool) {
    let sql = "SELECT s_name, r_name, '' FROM supplier, region;";
    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("region"))
        .project_multiple(MultiProjectBuilder::new("s_name", "s_name").add("r_name", "r_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT c_name, n_name, '' FROM
  (SELECT c_name, c_custkey FROM customer WHERE c_custkey = 1) c,
  nation;
*/
pub fn test_q33() -> (Query, String, bool) {
    let sql = r#"SELECT c_name, n_name, '' FROM
  (SELECT c_name, c_custkey FROM customer WHERE c_custkey = 1) c,
  nation;"#;
    let tiny_customer = QueryOp::scan("customer").filter(
        "c_custkey",
        ComparisionOperator::EQ,
        ComparisionValue::I32(1),
    );

    let query = tiny_customer
        .cross(QueryOp::scan("nation"))
        .project_multiple(MultiProjectBuilder::new("c_name", "c_name").add("n_name", "n_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT p_name, ps_availqty, '' FROM
  (SELECT p_name, p_partkey FROM part WHERE p_partkey = 10) p,
  (SELECT ps_availqty, ps_suppkey FROM partsupp WHERE ps_suppkey = 100) ps;
*/
pub fn test_q34() -> (Query, String, bool) {
    let sql = r#"SELECT p_name, ps_availqty, '' FROM
  (SELECT p_name, p_partkey FROM part WHERE p_partkey = 10) p,
  (SELECT ps_availqty, ps_suppkey FROM partsupp WHERE ps_suppkey = 100) ps;"#;
    let tiny_part = QueryOp::scan("part").filter(
        "p_partkey",
        ComparisionOperator::EQ,
        ComparisionValue::I32(10),
    );
    let tiny_partsupp = QueryOp::scan("partsupp").filter(
        "ps_suppkey",
        ComparisionOperator::EQ,
        ComparisionValue::I32(100),
    );

    let query = tiny_part
        .cross(tiny_partsupp)
        .project_multiple(
            MultiProjectBuilder::new("p_name", "p_name").add("ps_availqty", "ps_availqty"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT o_orderkey, l_linenumber, '' FROM
  (SELECT o_orderkey FROM orders WHERE o_orderkey = 5) o,
  (SELECT l_linenumber, l_orderkey FROM lineitem WHERE l_orderkey = 5) l;
*/
pub fn test_q35() -> (Query, String, bool) {
    let sql = r#"SELECT o_orderkey, l_linenumber, '' FROM
  (SELECT o_orderkey FROM orders WHERE o_orderkey = 5) o,
  (SELECT l_linenumber, l_orderkey FROM lineitem WHERE l_orderkey = 5) l;"#;
    let tiny_orders = QueryOp::scan("orders").filter(
        "o_orderkey",
        ComparisionOperator::EQ,
        ComparisionValue::I32(5),
    );
    let tiny_lineitem = QueryOp::scan("lineitem").filter(
        "l_orderkey",
        ComparisionOperator::EQ,
        ComparisionValue::I32(5),
    );

    let query = tiny_orders
        .cross(tiny_lineitem)
        .project_multiple(
            MultiProjectBuilder::new("o_orderkey", "o_orderkey")
                .add("l_linenumber", "l_linenumber"),
        )
        .build();
    (query, sql.to_string(), false)
}

// ============================================================================
// CATEGORY 6: SCAN + CROSS + FILTER + PROJECT
// Testing Inner Joins (Cross Joins followed by EQ filters).
// ============================================================================

/*
SELECT n_name, r_name, '' FROM nation, region WHERE n_regionkey = r_regionkey;
*/
pub fn test_q36() -> (Query, String, bool) {
    let sql = "SELECT n_name, r_name, '' FROM nation, region WHERE n_regionkey = r_regionkey;";
    let query = QueryOp::scan("nation")
        .cross(QueryOp::scan("region"))
        .filter(
            "n_regionkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("r_regionkey".to_string()),
        )
        .project_multiple(MultiProjectBuilder::new("n_name", "n_name").add("r_name", "r_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT c_name, n_name, '' FROM customer, nation WHERE c_nationkey = n_nationkey;
*/
pub fn test_q37() -> (Query, String, bool) {
    let sql = "SELECT c_name, n_name, '' FROM customer, nation WHERE c_nationkey = n_nationkey;";
    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("nation"))
        .filter(
            "c_nationkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("n_nationkey".to_string()),
        )
        .project_multiple(MultiProjectBuilder::new("c_name", "c_name").add("n_name", "n_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT s_name, n_name, '' FROM supplier, nation WHERE s_nationkey = n_nationkey;
*/
pub fn test_q38() -> (Query, String, bool) {
    let sql = "SELECT s_name, n_name, '' FROM supplier, nation WHERE s_nationkey = n_nationkey;";
    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("nation"))
        .filter(
            "s_nationkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("n_nationkey".to_string()),
        )
        .project_multiple(MultiProjectBuilder::new("s_name", "s_name").add("n_name", "n_name"))
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT p_name, ps_availqty, '' FROM part, partsupp WHERE p_partkey = ps_partkey;
*/
pub fn test_q39() -> (Query, String, bool) {
    let sql = "SELECT p_name, ps_availqty, '' FROM part, partsupp WHERE p_partkey = ps_partkey;";
    let query = QueryOp::scan("part")
        .cross(QueryOp::scan("partsupp"))
        .filter(
            "p_partkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("ps_partkey".to_string()),
        )
        .project_multiple(
            MultiProjectBuilder::new("p_name", "p_name").add("ps_availqty", "ps_availqty"),
        )
        .build();
    (query, sql.to_string(), false)
}

/*
SELECT s_name, ps_availqty, '' FROM supplier, partsupp WHERE s_suppkey = ps_suppkey;
*/
pub fn test_q40() -> (Query, String, bool) {
    let sql = "SELECT s_name, ps_availqty, '' FROM supplier, partsupp WHERE s_suppkey = ps_suppkey;";
    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("partsupp"))
        .filter(
            "s_suppkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("ps_suppkey".to_string()),
        )
        .project_multiple(
            MultiProjectBuilder::new("s_name", "s_name").add("ps_availqty", "ps_availqty"),
        )
        .build();
    (query, sql.to_string(), false)
}

// ============================================================================
// CATEGORY 7: SCAN + CROSS + FILTER + SORT + PROJECT
// The full relational pipeline: Joins, Predicates, Deterministic Sorting, Projections.
// ============================================================================

/*
SELECT c_name, o_totalprice, '' FROM customer, orders WHERE c_custkey = o_custkey AND c_mktsegment = 'MACHINERY' ORDER BY o_totalprice DESC, c_custkey, o_orderkey;
*/
pub fn test_q41() -> (Query, String, bool) {
    let sql = "SELECT c_name, o_totalprice, '' FROM customer, orders WHERE c_custkey = o_custkey AND c_mktsegment = 'MACHINERY' ORDER BY o_totalprice DESC, c_custkey, o_orderkey;";
    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_custkey".to_string()),
            )
            .add(
                "c_mktsegment",
                ComparisionOperator::EQ,
                ComparisionValue::String("MACHINERY".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("o_totalprice", false)
                .add("c_custkey", true)
                .add("o_orderkey", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("c_name", "c_name").add("o_totalprice", "o_totalprice"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT o_orderdate, l_quantity, '' FROM orders, lineitem WHERE o_orderkey = l_orderkey AND o_orderstatus = 'P' ORDER BY o_orderdate, l_orderkey, l_linenumber;
*/
pub fn test_q42() -> (Query, String, bool) {
    let sql = "SELECT o_orderdate, l_quantity, '' FROM orders, lineitem WHERE o_orderkey = l_orderkey AND o_orderstatus = 'P' ORDER BY o_orderdate, l_orderkey, l_linenumber;";
    let query = QueryOp::scan("orders")
        .cross(QueryOp::scan("lineitem"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "o_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("l_orderkey".to_string()),
            )
            .add(
                "o_orderstatus",
                ComparisionOperator::EQ,
                ComparisionValue::String("P".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("o_orderdate", true)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("o_orderdate", "o_orderdate").add("l_quantity", "l_quantity"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT n_name, r_name, '' FROM nation, region WHERE n_regionkey = r_regionkey AND r_name = 'EUROPE' ORDER BY n_name, n_nationkey;
*/
pub fn test_q43() -> (Query, String, bool) {
    let sql = "SELECT n_name, r_name, '' FROM nation, region WHERE n_regionkey = r_regionkey AND r_name = 'EUROPE' ORDER BY n_name, n_nationkey;";
    let query = QueryOp::scan("nation")
        .cross(QueryOp::scan("region"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("r_regionkey".to_string()),
            )
            .add(
                "r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("EUROPE".to_string()),
            ),
        )
        .sort_multiple(MultiSortBuilder::new("n_name", true).add("n_nationkey", true))
        .project_multiple(MultiProjectBuilder::new("n_name", "n_name").add("r_name", "r_name"))
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT s_name, n_name, '' FROM supplier, nation WHERE s_nationkey = n_nationkey AND s_acctbal > 1000.0 ORDER BY s_acctbal DESC, s_suppkey;
*/
pub fn test_q44() -> (Query, String, bool) {
    let sql = "SELECT s_name, n_name, '' FROM supplier, nation WHERE s_nationkey = n_nationkey AND s_acctbal > 1000.0 ORDER BY s_acctbal DESC, s_suppkey;";
    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("nation"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "s_acctbal",
                ComparisionOperator::GT,
                ComparisionValue::F64(1000.0),
            ),
        )
        .sort_multiple(MultiSortBuilder::new("s_acctbal", false).add("s_suppkey", true))
        .project_multiple(MultiProjectBuilder::new("s_name", "s_name").add("n_name", "n_name"))
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT p_name, ps_supplycost, '' FROM part, partsupp WHERE p_partkey = ps_partkey AND p_size = 50 ORDER BY ps_supplycost DESC, p_partkey, ps_suppkey;
*/
pub fn test_q45() -> (Query, String, bool) {
    let sql = "SELECT p_name, ps_supplycost, '' FROM part, partsupp WHERE p_partkey = ps_partkey AND p_size = 50 ORDER BY ps_supplycost DESC, p_partkey, ps_suppkey;";
    let query = QueryOp::scan("part")
        .cross(QueryOp::scan("partsupp"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "p_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_partkey".to_string()),
            )
            .add("p_size", ComparisionOperator::EQ, ComparisionValue::I32(50)),
        )
        .sort_multiple(
            MultiSortBuilder::new("ps_supplycost", false)
                .add("p_partkey", true)
                .add("ps_suppkey", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("p_name", "p_name").add("ps_supplycost", "ps_supplycost"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT c_name, n_name, '' FROM customer, nation, region WHERE c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' ORDER BY c_name, c_custkey;
*/
pub fn test_q46() -> (Query, String, bool) {
    let sql = "SELECT c_name, n_name, '' FROM customer, nation, region WHERE c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' ORDER BY c_name, c_custkey;";
    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("nation"))
        .cross(QueryOp::scan("region"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("r_regionkey".to_string()),
            )
            .add(
                "r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("ASIA".to_string()),
            ),
        )
        .sort_multiple(MultiSortBuilder::new("c_name", true).add("c_custkey", true))
        .project_multiple(MultiProjectBuilder::new("c_name", "c_name").add("n_name", "n_name"))
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT p_brand, s_name, '' FROM part, supplier, partsupp WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size < 5 ORDER BY p_brand, s_name, p_partkey, s_suppkey;
*/
pub fn test_q47() -> (Query, String, bool) {
    let sql = "SELECT p_brand, s_name, '' FROM part, supplier, partsupp WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size < 5 ORDER BY p_brand, s_name, p_partkey, s_suppkey;";
    let query = QueryOp::scan("part")
        .cross(QueryOp::scan("supplier"))
        .cross(QueryOp::scan("partsupp"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "p_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_partkey".to_string()),
            )
            .add(
                "s_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_suppkey".to_string()),
            )
            .add("p_size", ComparisionOperator::LT, ComparisionValue::I32(5)),
        )
        .sort_multiple(
            MultiSortBuilder::new("p_brand", true)
                .add("s_name", true)
                .add("p_partkey", true)
                .add("s_suppkey", true),
        )
        .project_multiple(MultiProjectBuilder::new("p_brand", "p_brand").add("s_name", "s_name"))
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT o_orderkey, c_mktsegment, '' FROM orders, customer WHERE o_custkey = c_custkey AND o_totalprice > 50000.0 ORDER BY o_totalprice DESC, o_orderkey;
*/
pub fn test_q48() -> (Query, String, bool) {
    let sql = "SELECT o_orderkey, c_mktsegment, '' FROM orders, customer WHERE o_custkey = c_custkey AND o_totalprice > 50000.0 ORDER BY o_totalprice DESC, o_orderkey;";
    let query = QueryOp::scan("orders")
        .cross(QueryOp::scan("customer"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "o_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("c_custkey".to_string()),
            )
            .add(
                "o_totalprice",
                ComparisionOperator::GT,
                ComparisionValue::F64(50000.0),
            ),
        )
        .sort_multiple(MultiSortBuilder::new("o_totalprice", false).add("o_orderkey", true))
        .project_multiple(
            MultiProjectBuilder::new("o_orderkey", "o_orderkey")
                .add("c_mktsegment", "c_mktsegment"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT l_linenumber, p_name, '' FROM lineitem, part WHERE l_partkey = p_partkey AND l_quantity > 45 ORDER BY p_name, l_orderkey, l_linenumber;
*/
pub fn test_q49() -> (Query, String, bool) {
    let sql = "SELECT l_linenumber, p_name, '' FROM lineitem, part WHERE l_partkey = p_partkey AND l_quantity > 45 ORDER BY p_name, l_orderkey, l_linenumber;";
    let query = QueryOp::scan("lineitem")
        .cross(QueryOp::scan("part"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "l_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("p_partkey".to_string()),
            )
            .add(
                "l_quantity",
                ComparisionOperator::GT,
                ComparisionValue::I32(45),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("p_name", true)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_linenumber", "l_linenumber").add("p_name", "p_name"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT s_name, p_name, '' FROM supplier, part, partsupp WHERE s_suppkey = ps_suppkey AND p_partkey = ps_partkey AND ps_availqty = 0 ORDER BY s_name, p_name, s_suppkey, p_partkey;
*/
pub fn test_q50() -> (Query, String, bool) {
    let sql = "SELECT s_name, p_name, '' FROM supplier, part, partsupp WHERE s_suppkey = ps_suppkey AND p_partkey = ps_partkey AND ps_availqty = 0 ORDER BY s_name, p_name, s_suppkey, p_partkey;";
    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("part"))
        .cross(QueryOp::scan("partsupp"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "s_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_suppkey".to_string()),
            )
            .add(
                "p_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_partkey".to_string()),
            )
            .add(
                "ps_availqty",
                ComparisionOperator::EQ,
                ComparisionValue::I32(0),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("s_name", true)
                .add("p_name", true)
                .add("s_suppkey", true)
                .add("p_partkey", true),
        )
        .project_multiple(MultiProjectBuilder::new("s_name", "s_name").add("p_name", "p_name"))
        .build();
    (query, sql.to_string(), true)
}

// ============================================================================
// Benchmark queries
// ============================================================================

/*
SELECT
    l_returnflag,
    l_linestatus,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    ''
FROM
    lineitem
WHERE
    l_shipdate <= '1998-11-30'
ORDER BY
    l_returnflag,
    l_linestatus,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;
*/
pub fn query_1() -> (Query, String, bool) {
    let sql = r#"SELECT
    l_returnflag,
    l_linestatus,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    ''
FROM
    lineitem
WHERE
    l_shipdate <= '1998-11-30'
ORDER BY
    l_returnflag,
    l_linestatus,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("lineitem")
        .filter(
            "l_shipdate",
            ComparisionOperator::LTE,
            ComparisionValue::String("1998-11-30".to_string()),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_returnflag", true)
                .add("l_linestatus", true)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_returnflag", "l_returnflag")
                .add("l_linestatus", "l_linestatus")
                .add("l_quantity", "l_quantity")
                .add("l_extendedprice", "l_extendedprice")
                .add("l_discount", "l_discount")
                .add("l_tax", "l_tax"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment,
    ps_supplycost,
    ''
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey,
    ps_suppkey      -- Deterministic tie-breaker
;
*/
pub fn query_2() -> (Query, String, bool) {
    let sql = r#"SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment,
    ps_supplycost,
    ''
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey,
    ps_suppkey      -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("part")
        .cross(QueryOp::scan("supplier"))
        .cross(QueryOp::scan("partsupp"))
        .cross(QueryOp::scan("nation"))
        .cross(QueryOp::scan("region"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "p_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_partkey".to_string()),
            )
            .add(
                "s_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_suppkey".to_string()),
            )
            .add("p_size", ComparisionOperator::EQ, ComparisionValue::I32(15))
            .add(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("r_regionkey".to_string()),
            )
            .add(
                "r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("EUROPE".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("s_acctbal", false)
                .add("n_name", true)
                .add("s_name", true)
                .add("p_partkey", true)
                .add("ps_suppkey", true),
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
                .add("ps_supplycost", "ps_supplycost"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority,
    ''
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
ORDER BY
    o_orderdate,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;
*/
pub fn query_3() -> (Query, String, bool) {
    let sql = r#"SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority,
    ''
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
ORDER BY
    o_orderdate,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .cross(QueryOp::scan("lineitem"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_mktsegment",
                ComparisionOperator::EQ,
                ComparisionValue::String("BUILDING".to_string()),
            )
            .add(
                "c_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_custkey".to_string()),
            )
            .add(
                "l_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_orderkey".to_string()),
            )
            .add(
                "o_orderdate",
                ComparisionOperator::LT,
                ComparisionValue::String("1995-03-15".to_string()),
            )
            .add(
                "l_shipdate",
                ComparisionOperator::GT,
                ComparisionValue::String("1995-03-15".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("o_orderdate", true)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("l_orderkey", "l_orderkey")
                .add("o_orderdate", "o_orderdate")
                .add("o_shippriority", "o_shippriority"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    n_name,
    l_extendedprice,
    l_discount,
    ''
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
ORDER BY
    l_extendedprice DESC,
    l_discount DESC,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;
*/
pub fn query_4() -> (Query, String, bool) {
    let sql = r#"SELECT
    n_name,
    l_extendedprice,
    l_discount,
    ''
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
ORDER BY
    l_extendedprice DESC,
    l_discount DESC,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .cross(QueryOp::scan("lineitem"))
        .cross(QueryOp::scan("supplier"))
        .cross(QueryOp::scan("nation"))
        .cross(QueryOp::scan("region"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_custkey".to_string()),
            )
            .add(
                "l_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_orderkey".to_string()),
            )
            .add(
                "l_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("s_suppkey".to_string()),
            )
            .add(
                "c_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("s_nationkey".to_string()),
            )
            .add(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("r_regionkey".to_string()),
            )
            .add(
                "r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("ASIA".to_string()),
            )
            .add(
                "o_orderdate",
                ComparisionOperator::GTE,
                ComparisionValue::String("1994-01-01".to_string()),
            )
            .add(
                "o_orderdate",
                ComparisionOperator::LT,
                ComparisionValue::String("1995-01-01".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_extendedprice", false)
                .add("l_discount", false)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("n_name", "n_name")
                .add("l_extendedprice", "l_extendedprice")
                .add("l_discount", "l_discount"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    l_extendedprice,
    l_discount,
    ''
FROM
    lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount > 0.05 AND l_discount < 0.07
    AND l_quantity < 24
ORDER BY
    l_orderkey,     -- Added to guarantee order
    l_linenumber    -- Added to guarantee order
;
*/
pub fn query_5() -> (Query, String, bool) {
    let sql = r#"SELECT
    l_extendedprice,
    l_discount,
    ''
FROM
    lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount > 0.05 AND l_discount < 0.07
    AND l_quantity < 24
ORDER BY
    l_orderkey,     -- Added to guarantee order
    l_linenumber    -- Added to guarantee order
;"#;

    let query = QueryOp::scan("lineitem")
        .filter_multiple(
            MultiPredicateBuilder::new(
                "l_shipdate",
                ComparisionOperator::GTE,
                ComparisionValue::String("1994-01-01".to_string()),
            )
            .add(
                "l_shipdate",
                ComparisionOperator::LT,
                ComparisionValue::String("1995-01-01".to_string()),
            )
            .add(
                "l_discount",
                ComparisionOperator::GT,
                ComparisionValue::F64(0.05),
            )
            .add(
                "l_discount",
                ComparisionOperator::LT,
                ComparisionValue::F64(0.07),
            )
            .add(
                "l_quantity",
                ComparisionOperator::LT,
                ComparisionValue::I32(24),
            ),
        )
        .sort_multiple(MultiSortBuilder::new("l_orderkey", true).add("l_linenumber", true))
        .project_multiple(
            MultiProjectBuilder::new("l_extendedprice", "l_extendedprice")
                .add("l_discount", "l_discount"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    c_custkey,
    c_name,
    l_extendedprice,
    l_discount,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment,
    ''
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-10-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
ORDER BY
    l_extendedprice desc,
    l_discount desc,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;
*/
pub fn query_6() -> (Query, String, bool) {
    let sql = r#"SELECT
    c_custkey,
    c_name,
    l_extendedprice,
    l_discount,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment,
    ''
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-10-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
ORDER BY
    l_extendedprice desc,
    l_discount desc,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .cross(QueryOp::scan("lineitem"))
        .cross(QueryOp::scan("nation"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_custkey".to_string()),
            )
            .add(
                "l_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_orderkey".to_string()),
            )
            .add(
                "o_orderdate",
                ComparisionOperator::GTE,
                ComparisionValue::String("1993-10-01".to_string()),
            )
            .add(
                "o_orderdate",
                ComparisionOperator::LT,
                ComparisionValue::String("1994-10-01".to_string()),
            )
            .add(
                "l_returnflag",
                ComparisionOperator::EQ,
                ComparisionValue::String("R".to_string()),
            )
            .add(
                "c_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("l_extendedprice", false)
                .add("l_discount", false)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("c_custkey", "c_custkey")
                .add("c_name", "c_name")
                .add("l_extendedprice", "l_extendedprice")
                .add("l_discount", "l_discount")
                .add("c_acctbal", "c_acctbal")
                .add("n_name", "n_name")
                .add("c_address", "c_address")
                .add("c_phone", "c_phone")
                .add("c_comment", "c_comment"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    ps_partkey,
    ps_supplycost,
    ps_availqty,
    ''
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
ORDER BY
    ps_availqty desc,
    ps_partkey,     -- Deterministic tie-breaker
    ps_suppkey      -- Deterministic tie-breaker
;
*/
pub fn query_7() -> (Query, String, bool) {
    let sql = r#"SELECT
    ps_partkey,
    ps_supplycost,
    ps_availqty,
    ''
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
ORDER BY
    ps_availqty desc,
    ps_partkey,     -- Deterministic tie-breaker
    ps_suppkey      -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("partsupp")
        .cross(QueryOp::scan("supplier"))
        .cross(QueryOp::scan("nation"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "ps_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("s_suppkey".to_string()),
            )
            .add(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("GERMANY".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("ps_availqty", false)
                .add("ps_partkey", true)
                .add("ps_suppkey", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("ps_partkey", "ps_partkey")
                .add("ps_supplycost", "ps_supplycost")
                .add("ps_availqty", "ps_availqty"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    s_name,
    s_address,
    s_suppkey,
    ps_partkey,
    ps_availqty,
    l_quantity,
    ''
FROM
    supplier,
    nation,
    partsupp,
    part,
    lineitem
WHERE
    s_nationkey = n_nationkey
    AND n_name = 'CANADA'
    AND s_suppkey = ps_suppkey
    AND ps_partkey = p_partkey
    AND l_partkey = ps_partkey
    AND l_suppkey = ps_suppkey
ORDER BY
    s_name,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;
*/
pub fn query_8() -> (Query, String, bool) {
    let sql = r#"SELECT
    s_name,
    s_address,
    s_suppkey,
    ps_partkey,
    ps_availqty,
    l_quantity,
    ''
FROM
    supplier,
    nation,
    partsupp,
    part,
    lineitem
WHERE
    s_nationkey = n_nationkey
    AND n_name = 'CANADA'
    AND s_suppkey = ps_suppkey
    AND ps_partkey = p_partkey
    AND l_partkey = ps_partkey
    AND l_suppkey = ps_suppkey
ORDER BY
    s_name,
    l_orderkey,     -- Deterministic tie-breaker
    l_linenumber    -- Deterministic tie-breaker
;"#;

    let query = QueryOp::scan("supplier")
        .cross(QueryOp::scan("nation"))
        .cross(QueryOp::scan("partsupp"))
        .cross(QueryOp::scan("part"))
        .cross(QueryOp::scan("lineitem"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("CANADA".to_string()),
            )
            .add(
                "s_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_suppkey".to_string()),
            )
            .add(
                "ps_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("p_partkey".to_string()),
            )
            .add(
                "l_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_partkey".to_string()),
            )
            .add(
                "l_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("ps_suppkey".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("s_name", true)
                .add("l_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("s_name", "s_name")
                .add("s_address", "s_address")
                .add("s_suppkey", "s_suppkey")
                .add("ps_partkey", "ps_partkey")
                .add("ps_availqty", "ps_availqty")
                .add("l_quantity", "l_quantity"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    s_name,
    l1.l_orderkey,
    l1.l_suppkey,
    ''
FROM
    supplier,
    lineitem l1,
    lineitem l2,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND l2.l_orderkey = l1.l_orderkey
    AND l2.l_suppkey <> l1.l_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
ORDER BY
    s_name,
    l1.l_orderkey,      -- Deterministic tie-breaker
    l1.l_linenumber,    -- Deterministic tie-breaker
    l2.l_orderkey,      -- Deterministic tie-breaker
    l2.l_linenumber     -- Deterministic tie-breaker
;
*/
pub fn query_9() -> (Query, String, bool) {
    let sql = r#"SELECT
    s_name,
    l1.l_orderkey,
    l1.l_suppkey,
    ''
FROM
    supplier,
    lineitem l1,
    lineitem l2,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND l2.l_orderkey = l1.l_orderkey
    AND l2.l_suppkey <> l1.l_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
ORDER BY
    s_name,
    l1.l_orderkey,      -- Deterministic tie-breaker
    l1.l_linenumber,    -- Deterministic tie-breaker
    l2.l_orderkey,      -- Deterministic tie-breaker
    l2.l_linenumber     -- Deterministic tie-breaker
;"#;

    let l1 = QueryOp::scan("lineitem").project_multiple(
        MultiProjectBuilder::new("l_orderkey", "l1.l_orderkey")
            .add("l_suppkey", "l1.l_suppkey")
            .add("l_receiptdate", "l1.l_receiptdate")
            .add("l_commitdate", "l1.l_commitdate")
            .add("l_linenumber", "l1.l_linenumber"),
    );
    let l2 = QueryOp::scan("lineitem").project_multiple(
        MultiProjectBuilder::new("l_orderkey", "l2.l_orderkey")
            .add("l_suppkey", "l2.l_suppkey")
            .add("l_linenumber", "l2.l_linenumber"),
    );

    let query = QueryOp::scan("supplier")
        .cross(l1)
        .cross(l2)
        .cross(QueryOp::scan("orders"))
        .cross(QueryOp::scan("nation"))
        .filter_multiple(
            MultiPredicateBuilder::new(
                "s_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("l1.l_suppkey".to_string()),
            )
            .add(
                "o_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("l1.l_orderkey".to_string()),
            )
            .add(
                "o_orderstatus",
                ComparisionOperator::EQ,
                ComparisionValue::String("F".to_string()),
            )
            .add(
                "l1.l_receiptdate",
                ComparisionOperator::GT,
                ComparisionValue::Column("l1.l_commitdate".to_string()),
            )
            .add(
                "l2.l_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("l1.l_orderkey".to_string()),
            )
            .add(
                "l2.l_suppkey",
                ComparisionOperator::NE,
                ComparisionValue::Column("l1.l_suppkey".to_string()),
            )
            .add(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("n_nationkey".to_string()),
            )
            .add(
                "n_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("SAUDI ARABIA".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("s_name", true)
                .add("l1.l_orderkey", true)
                .add("l1.l_linenumber", true)
                .add("l2.l_orderkey", true)
                .add("l2.l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("s_name", "s_name")
                .add("l1.l_orderkey", "l1.l_orderkey")
                .add("l1.l_suppkey", "l1.l_suppkey"),
        )
        .build();
    (query, sql.to_string(), true)
}

/*
SELECT
    c_custkey,
    c_name,
    o_orderkey,
    o_orderdate,
    l_linenumber,
    l_partkey,
    l_suppkey,
    p_name,
    s_name,
    cn.n_name,
    sn.n_name,
    ''
FROM
    customer,
    orders,
    lineitem,
    part,
    supplier,
    nation cn,
    nation sn,
    region cr,
    region sr
WHERE
    c_custkey = o_custkey
    AND o_orderkey = l_orderkey
    AND l_partkey = p_partkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = cn.n_nationkey
    AND s_nationkey = sn.n_nationkey
    AND cn.n_regionkey = cr.r_regionkey
    AND sn.n_regionkey = sr.r_regionkey
    AND c_mktsegment = 'BUILDING'
    AND l_shipmode = 'AIR'
    AND l_shipinstruct = 'DELIVER IN PERSON'
    AND p_brand = 'Brand#12'
    AND p_size >= 1
    AND p_size <= 5
    AND cr.r_name = 'ASIA'
    AND sr.r_name = 'EUROPE'
ORDER BY
    c_custkey,
    o_orderdate,
    o_orderkey,         -- Already deterministic (PK of order)
    l_linenumber        -- Already deterministic (PK of lineitem)
;
*/
pub fn query_10() -> (Query, String, bool) {
    let sql = r#"SELECT
    c_custkey,
    c_name,
    o_orderkey,
    o_orderdate,
    l_linenumber,
    l_partkey,
    l_suppkey,
    p_name,
    s_name,
    cn.n_name,
    sn.n_name,
    ''
FROM
    customer,
    orders,
    lineitem,
    part,
    supplier,
    nation cn,
    nation sn,
    region cr,
    region sr
WHERE
    c_custkey = o_custkey
    AND o_orderkey = l_orderkey
    AND l_partkey = p_partkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = cn.n_nationkey
    AND s_nationkey = sn.n_nationkey
    AND cn.n_regionkey = cr.r_regionkey
    AND sn.n_regionkey = sr.r_regionkey
    AND c_mktsegment = 'BUILDING'
    AND l_shipmode = 'AIR'
    AND l_shipinstruct = 'DELIVER IN PERSON'
    AND p_brand = 'Brand#12'
    AND p_size >= 1
    AND p_size <= 5
    AND cr.r_name = 'ASIA'
    AND sr.r_name = 'EUROPE'
ORDER BY
    c_custkey,
    o_orderdate,
    o_orderkey,         -- Already deterministic (PK of order)
    l_linenumber        -- Already deterministic (PK of lineitem)
;"#;

    let cn = QueryOp::scan("nation").project_multiple(
        MultiProjectBuilder::new("n_nationkey", "cn.n_nationkey")
            .add("n_regionkey", "cn.n_regionkey")
            .add("n_name", "cn.n_name"),
    );
    let sn = QueryOp::scan("nation").project_multiple(
        MultiProjectBuilder::new("n_nationkey", "sn.n_nationkey")
            .add("n_regionkey", "sn.n_regionkey")
            .add("n_name", "sn.n_name"),
    );
    let cr = QueryOp::scan("region").project_multiple(
        MultiProjectBuilder::new("r_regionkey", "cr.r_regionkey").add("r_name", "cr.r_name"),
    );
    let sr = QueryOp::scan("region").project_multiple(
        MultiProjectBuilder::new("r_regionkey", "sr.r_regionkey").add("r_name", "sr.r_name"),
    );

    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .cross(QueryOp::scan("lineitem"))
        .cross(QueryOp::scan("part"))
        .cross(QueryOp::scan("supplier"))
        .cross(cn)
        .cross(sn)
        .cross(cr)
        .cross(sr)
        .filter_multiple(
            MultiPredicateBuilder::new(
                "c_custkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("o_custkey".to_string()),
            )
            .add(
                "o_orderkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("l_orderkey".to_string()),
            )
            .add(
                "l_partkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("p_partkey".to_string()),
            )
            .add(
                "l_suppkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("s_suppkey".to_string()),
            )
            .add(
                "c_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("cn.n_nationkey".to_string()),
            )
            .add(
                "s_nationkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("sn.n_nationkey".to_string()),
            )
            .add(
                "cn.n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("cr.r_regionkey".to_string()),
            )
            .add(
                "sn.n_regionkey",
                ComparisionOperator::EQ,
                ComparisionValue::Column("sr.r_regionkey".to_string()),
            )
            .add(
                "c_mktsegment",
                ComparisionOperator::EQ,
                ComparisionValue::String("BUILDING".to_string()),
            )
            .add(
                "l_shipmode",
                ComparisionOperator::EQ,
                ComparisionValue::String("AIR".to_string()),
            )
            .add(
                "l_shipinstruct",
                ComparisionOperator::EQ,
                ComparisionValue::String("DELIVER IN PERSON".to_string()),
            )
            .add(
                "p_brand",
                ComparisionOperator::EQ,
                ComparisionValue::String("Brand#12".to_string()),
            )
            .add("p_size", ComparisionOperator::GTE, ComparisionValue::I32(1))
            .add("p_size", ComparisionOperator::LTE, ComparisionValue::I32(5))
            .add(
                "cr.r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("ASIA".to_string()),
            )
            .add(
                "sr.r_name",
                ComparisionOperator::EQ,
                ComparisionValue::String("EUROPE".to_string()),
            ),
        )
        .sort_multiple(
            MultiSortBuilder::new("c_custkey", true)
                .add("o_orderdate", true)
                .add("o_orderkey", true)
                .add("l_linenumber", true),
        )
        .project_multiple(
            MultiProjectBuilder::new("c_custkey", "c_custkey")
                .add("c_name", "c_name")
                .add("o_orderkey", "o_orderkey")
                .add("o_orderdate", "o_orderdate")
                .add("l_linenumber", "l_linenumber")
                .add("l_partkey", "l_partkey")
                .add("l_suppkey", "l_suppkey")
                .add("p_name", "p_name")
                .add("s_name", "s_name")
                .add("cn.n_name", "cn.n_name")
                .add("sn.n_name", "sn.n_name"),
        )
        .build();
    (query, sql.to_string(), true)
}
