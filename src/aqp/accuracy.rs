pub struct AccuracyCalculator;

impl AccuracyCalculator {
    pub fn calculate_sampling_rate(accuracy_target: f64, k: f64) -> f64 {
        if accuracy_target >= 1.0 {
            return 1.0;
        }
        if accuracy_target <= 0.0 {
            return 0.0001;
        }
        // Simple power law: high accuracy requires high sampling
        // e.g., 0.9 accuracy with k=2 might need 0.9^2 = 0.81 sampling
        accuracy_target.powf(k).max(0.0001).min(1.0)
    }

    pub fn split_sampling_rate(sampling_rate: f64) -> (f64, f64) {
        let sst_rate = sampling_rate.sqrt();
        let row_rate = sampling_rate.sqrt();
        (sst_rate, row_rate)
    }

    pub fn calculate_required_sample_size(variance: f64, accuracy_target: f64, k: f64) -> f64 {
        if variance == 0.0 {
            return 100.0;
        } // Minimum sample
        // Simplified formula for required sample size based on variance and target error
        let target_error = 1.0 - accuracy_target;
        let n = (k * variance.sqrt() / target_error).powi(2);
        n.max(100.0)
    }
}
