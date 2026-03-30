use crate::query::ast::QueryPlan;

#[derive(Debug, Clone)]
pub enum ExecutionPath {
    Row {
        sstables: Vec<String>,
    },
    Columnar {
        segments: Vec<String>,
        columns_needed: Vec<String>,
    },
    FastPath {
        segments: Vec<String>,
        column: Option<String>,
        aggregation: crate::query::ast::Aggregation,
    },
    Mixed {
        sstables: Vec<String>,
        segments: Vec<String>,
        columns_needed: Vec<String>,
    },
}

use crate::storage::manager::StorageManager;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(plan: &QueryPlan, storage: &StorageManager) -> ExecutionPath {
        let segments = storage.get_all_segments();
        let sstables = storage.get_all_sstables();

        let mut columns_needed = Vec::new();
        match &plan.aggregation {
            crate::query::ast::Aggregation::Count => {
                columns_needed.push("user_id".to_string());
            }
            crate::query::ast::Aggregation::Sum(col) | crate::query::ast::Aggregation::Avg(col) => {
                columns_needed.push(col.clone());
            }
        }

        if let Some(filter) = &plan.filter {
            if !columns_needed.contains(&filter.column) {
                columns_needed.push(filter.column.clone());
            }
        }

        if let Some(group_by) = &plan.group_by {
            if !columns_needed.contains(group_by) {
                columns_needed.push(group_by.clone());
            }
        }

        if plan.filter.is_none() && plan.group_by.is_none() && sstables.is_empty() {
            let col = match &plan.aggregation {
                crate::query::ast::Aggregation::Count => None,
                crate::query::ast::Aggregation::Sum(c) | crate::query::ast::Aggregation::Avg(c) => {
                    Some(c.clone())
                }
            };
            return ExecutionPath::FastPath {
                segments,
                column: col,
                aggregation: plan.aggregation.clone(),
            };
        }

        if segments.is_empty() {
            ExecutionPath::Row { sstables }
        } else if sstables.is_empty() {
            ExecutionPath::Columnar {
                segments,
                columns_needed,
            }
        } else {
            ExecutionPath::Mixed {
                sstables,
                segments,
                columns_needed,
            }
        }
    }
}
