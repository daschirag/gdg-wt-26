use crate::query::ast::QueryPlan;
use crate::storage::columnar::segment::Segment;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum ExecutionPath {
    Row { sstables: Vec<String> },
    Columnar { segments: Vec<String>, columns_needed: Vec<String> },
}

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(plan: &QueryPlan, sst_dir: &Path) -> ExecutionPath {
        let segments_dir = sst_dir.join("segments");
        let mut segments = Vec::new();

        if segments_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(segments_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if entry.path().is_dir() {
                            segments.push(entry.path().to_str().unwrap().to_string());
                        }
                    }
                }
            }
        }

        // If no segments exist, we MUST use row path
        if segments.is_empty() {
            let mut sstables = Vec::new();
            if let Ok(entries) = std::fs::read_dir(sst_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.extension().map_or(false, |ext| ext == "aqe") {
                            sstables.push(path.to_str().unwrap().to_string());
                        }
                    }
                }
            }
            return ExecutionPath::Row { sstables };
        }

        // Columnar Decision Rules:
        // 1. If query touches only 1-2 columns -> Columnar
        // 2. If global COUNT(*) -> Columnar
        // 3. For now, we'll favor columnar if segments exist.
        
        let mut columns_needed = Vec::new();
        match &plan.aggregation {
            crate::query::ast::Aggregation::Count => {
                // For COUNT(*), we just need any column. We'll pick user_id as it's likely the first.
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

        ExecutionPath::Columnar { segments, columns_needed }
    }
}
