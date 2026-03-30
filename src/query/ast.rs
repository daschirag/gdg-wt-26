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

impl QueryPlan {
    pub fn required_columns(&self) -> Vec<String> {
        let mut cols = Vec::new();
        match &self.aggregation {
            Aggregation::Count => {}
            Aggregation::Sum(c) | Aggregation::Avg(c) => cols.push(c.clone()),
        }
        if let Some(f) = &self.filter {
            if !cols.contains(&f.column) {
                cols.push(f.column.clone());
            }
        }
        if let Some(g) = &self.group_by {
            if !cols.contains(g) {
                cols.push(g.clone());
            }
        }
        cols
    }
}
