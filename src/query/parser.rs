use crate::errors::QueryError;
use crate::query::ast::{Aggregation, Filter, FilterOp, PredicateExpr, QueryPlan};

pub struct Parser;

impl Parser {
    pub fn parse(sql: &str) -> Result<QueryPlan, QueryError> {
        let sql = sql.trim().trim_end_matches(';');
        let upper = sql.to_ascii_uppercase();
        if !upper.starts_with("SELECT ") {
            return Err(QueryError::UnsupportedOperation(
                "Only SELECT is supported".to_string(),
            ));
        }
        let from_pos = upper
            .find(" FROM ")
            .ok_or_else(|| QueryError::ParseError("Expected FROM".to_string()))?;
        let agg_raw = sql["SELECT ".len()..from_pos].trim();
        let tail = sql[from_pos + " FROM ".len()..].trim();

        let tokens: Vec<&str> = tail.split_whitespace().collect();
        if tokens.is_empty() {
            return Err(QueryError::ParseError("Query too short".to_string()));
        }

        let agg_upper = agg_raw.to_ascii_uppercase();
        let aggregation = if agg_upper == "COUNT(*)" {
            Aggregation::Count
        } else if agg_upper.starts_with("SUM(") && agg_upper.ends_with(")") {
            Aggregation::Sum(agg_raw[4..agg_raw.len() - 1].to_string())
        } else if agg_upper.starts_with("AVG(") && agg_upper.ends_with(")") {
            Aggregation::Avg(agg_raw[4..agg_raw.len() - 1].to_string())
        } else if agg_upper.starts_with("APPROX_PERCENTILE(") && agg_upper.ends_with(")") {
            let inner = &agg_raw["APPROX_PERCENTILE(".len()..agg_raw.len() - 1];
            let mut parts = inner.splitn(2, ',');
            let column = parts
                .next()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    QueryError::ParseError(
                        "APPROX_PERCENTILE expects a column and quantile".to_string(),
                    )
                })?;
            let quantile = parts
                .next()
                .map(str::trim)
                .ok_or_else(|| {
                    QueryError::ParseError(
                        "APPROX_PERCENTILE expects a column and quantile".to_string(),
                    )
                })?
                .parse::<f64>()
                .map_err(|_| {
                    QueryError::ParseError(
                        "APPROX_PERCENTILE quantile must be numeric".to_string(),
                    )
                })?;
            if !(0.0..=1.0).contains(&quantile) {
                return Err(QueryError::ParseError(
                    "APPROX_PERCENTILE quantile must be in [0, 1]".to_string(),
                ));
            }
            Aggregation::ApproxPercentile(column.to_string(), quantile)
        } else {
            return Err(QueryError::UnsupportedOperation(format!(
                "Unsupported aggregation: {}",
                agg_raw
            )));
        };
        let table = tokens[0].to_string();

        let mut filter = None;
        let mut group_by = None;
        let mut limit: Option<u64> = None;
        let mut offset: Option<u64> = None;
        let mut current = 1;

        while current < tokens.len() {
            let token_upper = tokens[current].to_uppercase();
            if token_upper == "WHERE" {
                current += 1;
                let clause_end = find_clause_end(&tokens[current..]).map(|i| current + i);
                let end = clause_end.unwrap_or(tokens.len());
                filter = Some(parse_expr(&tokens[current..end])?);
                current = end;
            } else if token_upper == "GROUP"
                && current + 2 < tokens.len()
                && tokens[current + 1].to_uppercase() == "BY"
            {
                group_by = Some(tokens[current + 2].to_string());
                current += 3;
            } else if token_upper == "LIMIT" {
                current += 1;
                if current >= tokens.len() {
                    return Err(QueryError::ParseError("LIMIT requires a value".to_string()));
                }
                limit = Some(tokens[current].parse::<u64>().map_err(|_| {
                    QueryError::ParseError(format!("Invalid LIMIT value: {}", tokens[current]))
                })?);
                current += 1;
            } else if token_upper == "OFFSET" {
                current += 1;
                if current >= tokens.len() {
                    return Err(QueryError::ParseError("OFFSET requires a value".to_string()));
                }
                offset = Some(tokens[current].parse::<u64>().map_err(|_| {
                    QueryError::ParseError(format!("Invalid OFFSET value: {}", tokens[current]))
                })?);
                current += 1;
            } else {
                return Err(QueryError::ParseError(format!(
                    "Unexpected token: {}",
                    tokens[current]
                )));
            }
        }

        Ok(QueryPlan {
            aggregation,
            table,
            filter,
            group_by,
            limit,
            offset,
        })
    }
}

/// Returns the index of the first token that starts a new clause (GROUP BY, LIMIT, OFFSET).
fn find_clause_end(tokens: &[&str]) -> Option<usize> {
    for (i, token) in tokens.iter().enumerate() {
        let up = token.to_ascii_uppercase();
        if up == "LIMIT" || up == "OFFSET" {
            return Some(i);
        }
        if up == "GROUP" {
            if tokens.get(i + 1).map(|t| t.eq_ignore_ascii_case("BY")).unwrap_or(false) {
                return Some(i);
            }
        }
    }
    None
}

fn parse_expr(tokens: &[&str]) -> Result<PredicateExpr, QueryError> {
    if tokens.is_empty() {
        return Err(QueryError::ParseError("Incomplete WHERE clause".to_string()));
    }
    parse_or(tokens)
}

fn parse_or(tokens: &[&str]) -> Result<PredicateExpr, QueryError> {
    let mut parts = split_top_level(tokens, "OR");
    let first = parse_and(parts.remove(0))?;
    parts.into_iter().try_fold(first, |left, part| {
        Ok(PredicateExpr::Or(Box::new(left), Box::new(parse_and(part)?)))
    })
}

fn parse_and(tokens: &[&str]) -> Result<PredicateExpr, QueryError> {
    let mut parts = split_top_level(tokens, "AND");
    let first = parse_not(parts.remove(0))?;
    parts.into_iter().try_fold(first, |left, part| {
        Ok(PredicateExpr::And(Box::new(left), Box::new(parse_not(part)?)))
    })
}

fn parse_not(tokens: &[&str]) -> Result<PredicateExpr, QueryError> {
    if tokens.is_empty() {
        return Err(QueryError::ParseError("Incomplete predicate".to_string()));
    }
    if tokens[0].eq_ignore_ascii_case("NOT") {
        return Ok(PredicateExpr::Not(Box::new(parse_not(&tokens[1..])?)));
    }
    parse_comparison(tokens)
}

fn parse_comparison(tokens: &[&str]) -> Result<PredicateExpr, QueryError> {
    if tokens.len() != 3 {
        return Err(QueryError::ParseError(format!(
            "Expected <column> <op> <value>, got {:?}",
            tokens
        )));
    }
    let op = match tokens[1] {
        "=" => FilterOp::Eq,
        ">" => FilterOp::Gt,
        "<" => FilterOp::Lt,
        ">=" => FilterOp::Ge,
        "<=" => FilterOp::Le,
        other => {
            return Err(QueryError::UnsupportedOperation(format!(
                "Unsupported operator: {}",
                other
            )))
        }
    };
    Ok(PredicateExpr::Comparison(Filter {
        column: tokens[0].to_string(),
        op,
        value: tokens[2].to_string(),
    }))
}

fn split_top_level<'a>(tokens: &'a [&'a str], keyword: &str) -> Vec<&'a [&'a str]> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (idx, token) in tokens.iter().enumerate() {
        if token.eq_ignore_ascii_case(keyword) {
            parts.push(&tokens[start..idx]);
            start = idx + 1;
        }
    }
    parts.push(&tokens[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::query::ast::{Aggregation, PredicateExpr};

    #[test]
    fn parses_and_precedence_over_or() {
        let plan = Parser::parse("SELECT COUNT(*) FROM logs WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        match plan.filter.unwrap() {
            PredicateExpr::Or(_, right) => match *right {
                PredicateExpr::And(_, _) => {}
                other => panic!("expected AND on right branch, got {:?}", other),
            },
            other => panic!("expected OR root, got {:?}", other),
        }
    }

    #[test]
    fn parses_not_before_and() {
        let plan = Parser::parse("SELECT COUNT(*) FROM logs WHERE NOT a = 1 AND b = 2").unwrap();
        match plan.filter.unwrap() {
            PredicateExpr::And(left, _) => match *left {
                PredicateExpr::Not(_) => {}
                other => panic!("expected NOT on left branch, got {:?}", other),
            },
            other => panic!("expected AND root, got {:?}", other),
        }
    }

    #[test]
    fn parses_approx_percentile() {
        let plan =
            Parser::parse("SELECT APPROX_PERCENTILE(level, 0.95) FROM logs WHERE status = 0")
                .unwrap();
        match plan.aggregation {
            Aggregation::ApproxPercentile(column, quantile) => {
                assert_eq!(column, "level");
                assert!((quantile - 0.95).abs() < f64::EPSILON);
            }
            other => panic!("expected approx percentile, got {:?}", other),
        }
    }
}
