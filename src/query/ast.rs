#[derive(Debug, Clone)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub aggregation: Aggregation,
    pub table: String,
    pub filter: Option<Filter>,
    pub group_by: Option<String>,
}
