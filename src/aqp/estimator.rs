use crate::query::ast::Aggregation;
use crate::types::AggregateValue;

pub struct Estimator;

impl Estimator {
    pub fn scale_result(
        value: AggregateValue,
        aggregation: &Aggregation,
        sampling_rate: f64,
    ) -> AggregateValue {
        if sampling_rate >= 1.0 {
            return value;
        }

        match value {
            AggregateValue::Scalar(val) => {
                match aggregation {
                    Aggregation::Count | Aggregation::Sum(_) => {
                        AggregateValue::Scalar(val * (1.0 / sampling_rate))
                    }
                    Aggregation::Avg(_) => AggregateValue::Scalar(val), // Avg doesn't scale
                }
            }
            AggregateValue::Groups(groups) => {
                let scaled_groups = groups
                    .into_iter()
                    .map(|(k, v, conf)| {
                        let scaled_v = match aggregation {
                            Aggregation::Count | Aggregation::Sum(_) => v * (1.0 / sampling_rate),
                            Aggregation::Avg(_) => v,
                        };
                        (k, scaled_v, conf)
                    })
                    .collect();
                AggregateValue::Groups(scaled_groups)
            }
            AggregateValue::Empty => AggregateValue::Empty,
        }
    }
}
