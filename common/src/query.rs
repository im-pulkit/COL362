use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    pub root: QueryOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ComparisionOperator {
    EQ,
    NE,
    GT,
    GTE,
    LT,
    LTE,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ComparisionValue {
    Column(String),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Predicate {
    pub column_name: String,
    pub operator: ComparisionOperator,
    pub value: ComparisionValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterData {
    pub predicates: Vec<Predicate>,
    pub underlying: Box<QueryOp>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectData {
    pub column_name_map: Vec<(String, String)>,
    pub underlying: Box<QueryOp>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrossData {
    pub left: Box<QueryOp>,
    pub right: Box<QueryOp>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortSpec {
    pub column_name: String,
    pub ascending: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct SortData {
    pub sort_specs: Vec<SortSpec>,
    pub underlying: Box<QueryOp>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanData {
    pub table_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryOp {
    Filter(FilterData),
    Project(ProjectData),
    Cross(CrossData),
    Sort(SortData),
    Scan(ScanData),
}

impl QueryOp {
    pub fn scan(table_id: &str) -> QueryOp {
        QueryOp::Scan(ScanData {
            table_id: String::from(table_id),
        })
    }

    pub fn filter_multiple(self, multi_predicate_builder: MultiPredicateBuilder) -> QueryOp {
        QueryOp::Filter(FilterData {
            underlying: Box::new(self),
            predicates: multi_predicate_builder.build(),
        })
    }

    pub fn filter(
        self,
        column_name: &str,
        operator: ComparisionOperator,
        value: ComparisionValue,
    ) -> QueryOp {
        Self::filter_multiple(
            self,
            MultiPredicateBuilder::new(column_name, operator, value),
        )
    }

    pub fn cross(self, right: QueryOp) -> QueryOp {
        QueryOp::Cross(CrossData {
            left: Box::new(self),
            right: Box::new(right),
        })
    }

    pub fn sort_multiple(self, multi_sort_builder: MultiSortBuilder) -> QueryOp {
        QueryOp::Sort(SortData {
            underlying: Box::new(self),
            sort_specs: multi_sort_builder.build(),
        })
    }

    pub fn sort(self, column_name: &str, ascending: bool) -> QueryOp {
        Self::sort_multiple(self, MultiSortBuilder::new(column_name, ascending))
    }

    pub fn project(self, from: &str, to: &str) -> QueryOp {
        QueryOp::Project(ProjectData {
            underlying: Box::new(self),
            column_name_map: MultiProjectBuilder::new(from, to).build(),
        })
    }

    pub fn project_multiple(self, multi_project_builder: MultiProjectBuilder) -> QueryOp {
        QueryOp::Project(ProjectData {
            underlying: Box::new(self),
            column_name_map: multi_project_builder.build(),
        })
    }

    pub fn build(self) -> Query {
        Query { root: self }
    }
}

pub struct MultiProjectBuilder {
    column_name_map: Vec<(String, String)>,
}

impl MultiProjectBuilder {
    pub fn new(from: &str, to: &str) -> Self {
        Self {
            column_name_map: vec![(String::from(from), String::from(to))],
        }
    }

    pub fn add(mut self, from: &str, to: &str) -> Self {
        self.column_name_map
            .push((String::from(from), String::from(to)));
        self
    }

    fn build(self) -> Vec<(String, String)> {
        self.column_name_map
    }
}

pub struct MultiSortBuilder {
    sort_specs: Vec<SortSpec>,
}

impl MultiSortBuilder {
    pub fn new(column_name: &str, ascending: bool) -> MultiSortBuilder {
        MultiSortBuilder {
            sort_specs: vec![SortSpec {
                column_name: String::from(column_name),
                ascending,
            }],
        }
    }

    pub fn add(mut self, column_name: &str, ascending: bool) -> MultiSortBuilder {
        self.sort_specs.push(SortSpec {
            column_name: String::from(column_name),
            ascending,
        });
        self
    }

    fn build(self) -> Vec<SortSpec> {
        self.sort_specs
    }
}

pub struct MultiPredicateBuilder {
    predicates: Vec<Predicate>,
}

impl MultiPredicateBuilder {
    pub fn new(
        column_name: &str,
        operator: ComparisionOperator,
        value: ComparisionValue,
    ) -> MultiPredicateBuilder {
        MultiPredicateBuilder {
            predicates: vec![Predicate {
                column_name: String::from(column_name),
                operator,
                value,
            }],
        }
    }
    pub fn add(
        mut self,
        column_name: &str,
        operator: ComparisionOperator,
        value: ComparisionValue,
    ) -> MultiPredicateBuilder {
        self.predicates.push(Predicate {
            column_name: String::from(column_name),
            operator,
            value,
        });
        self
    }

    fn build(self) -> Vec<Predicate> {
        self.predicates
    }
}
