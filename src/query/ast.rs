use crate::types::Value;

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
pub enum PredicateExpr {
    Comparison(Filter),
    Not(Box<PredicateExpr>),
    And(Box<PredicateExpr>, Box<PredicateExpr>),
    Or(Box<PredicateExpr>, Box<PredicateExpr>),
}

impl PredicateExpr {
    pub fn collect_columns(&self, out: &mut Vec<String>) {
        match self {
            PredicateExpr::Comparison(filter) => {
                if !out.contains(&filter.column) {
                    out.push(filter.column.clone());
                }
            }
            PredicateExpr::Not(inner) => inner.collect_columns(out),
            PredicateExpr::And(left, right) | PredicateExpr::Or(left, right) => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
        }
    }

    pub fn eval_value(&self, get_value: &impl Fn(&str) -> Option<Value>) -> bool {
        match self {
            PredicateExpr::Comparison(filter) => get_value(&filter.column)
                .map(|value| compare_value(&value, filter))
                .unwrap_or(false),
            PredicateExpr::Not(inner) => !inner.eval_value(get_value),
            PredicateExpr::And(left, right) => {
                left.eval_value(get_value) && right.eval_value(get_value)
            }
            PredicateExpr::Or(left, right) => left.eval_value(get_value) || right.eval_value(get_value),
        }
    }
}

fn compare_value(value: &Value, filter: &Filter) -> bool {
    let filter_value = match value {
        Value::Int(_) => filter
            .value
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::String(filter.value.clone())),
        Value::Float(_) => filter
            .value
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::String(filter.value.clone())),
        Value::String(_) => Value::String(filter.value.clone()),
    };

    match filter.op {
        FilterOp::Eq => value == &filter_value,
        FilterOp::Gt => value > &filter_value,
        FilterOp::Lt => value < &filter_value,
        FilterOp::Ge => value >= &filter_value,
        FilterOp::Le => value <= &filter_value,
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub aggregation: Aggregation,
    pub table: String,
    pub filter: Option<PredicateExpr>,
    pub group_by: Option<String>,
}

impl QueryPlan {
    pub fn required_columns(&self) -> Vec<String> {
        let mut cols = Vec::new();
        match &self.aggregation {
            Aggregation::Count => {}
            Aggregation::Sum(c) | Aggregation::Avg(c) => cols.push(c.clone()),
        }
        if let Some(filter) = &self.filter {
            filter.collect_columns(&mut cols);
        }
        if let Some(g) = &self.group_by {
            if !cols.contains(g) {
                cols.push(g.clone());
            }
        }
        cols
    }
}
