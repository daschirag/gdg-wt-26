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

        let columns_needed = plan.required_columns();

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
