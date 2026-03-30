use crate::query::ast::{QueryPlan, Aggregation, Filter, FilterOp};
use crate::errors::QueryError;

pub struct Parser;

impl Parser {
    pub fn parse(sql: &str) -> Result<QueryPlan, QueryError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 4 {
            return Err(QueryError::ParseError("Query too short".to_string()));
        }

        // Simplistic parser: SELECT <agg> FROM <table> [WHERE <col> <op> <val>] [GROUP BY <col>]
        if tokens[0].to_uppercase() != "SELECT" {
            return Err(QueryError::UnsupportedOperation("Only SELECT is supported".to_string()));
        }

        let agg_raw = tokens[1];
        let agg_upper = agg_raw.to_uppercase();
        let aggregation = if agg_upper == "COUNT(*)" {
            Aggregation::Count
        } else if agg_upper.starts_with("SUM(") && agg_upper.ends_with(")") {
            let col = agg_raw[4..agg_raw.len()-1].to_string();
            Aggregation::Sum(col)
        } else if agg_upper.starts_with("AVG(") && agg_upper.ends_with(")") {
            let col = agg_raw[4..agg_raw.len()-1].to_string();
            Aggregation::Avg(col)
        } else {
            return Err(QueryError::UnsupportedOperation(format!("Unsupported aggregation: {}", agg_raw)));
        };

        if tokens[2].to_uppercase() != "FROM" {
            return Err(QueryError::ParseError("Expected FROM".to_string()));
        }

        let table = tokens[3].to_string();

        let mut filter = None;
        let mut group_by = None;

        let mut current = 4;
        while current < tokens.len() {
            let token_upper = tokens[current].to_uppercase();
            if token_upper == "WHERE" {
                if current + 3 >= tokens.len() {
                    return Err(QueryError::ParseError("Incomplete WHERE clause".to_string()));
                }
                let column = tokens[current + 1].to_string();
                let op_str = tokens[current + 2];
                let op = match op_str {
                    "=" => FilterOp::Eq,
                    ">" => FilterOp::Gt,
                    "<" => FilterOp::Lt,
                    ">=" => FilterOp::Ge,
                    "<=" => FilterOp::Le,
                    _ => return Err(QueryError::UnsupportedOperation(format!("Unsupported operator: {}", op_str))),
                };
                let value = tokens[current + 3].to_string();
                filter = Some(Filter { column, op, value });
                current += 4;
            } else if token_upper == "GROUP" && current + 2 < tokens.len() && tokens[current + 1].to_uppercase() == "BY" {
                group_by = Some(tokens[current + 2].to_string());
                current += 3;
            } else {
                return Err(QueryError::ParseError(format!("Unexpected token: {}", tokens[current])));
            }
        }

        Ok(QueryPlan {
            aggregation,
            table,
            filter,
            group_by,
        })
    }
}
