pub struct AccuracyCalculator;

impl AccuracyCalculator {
    pub fn calculate_sampling_rate(accuracy_target: f64, k: f64) -> f64 {
        // sampling_rate = (1.0 - accuracy_target).powf(k)
        let rate = (1.0 - accuracy_target).powf(k);
        
        // Clamp to [0.05, 1.0] and warn (warn handled in calling module)
        if rate < 0.05 {
            0.05
        } else if rate > 1.0 {
            1.0
        } else {
            rate
        }
    }

    pub fn split_sampling_rate(sampling_rate: f64) -> (f64, f64) {
        let sst_rate = sampling_rate.sqrt();
        let row_rate = sampling_rate.sqrt();
        (sst_rate, row_rate)
    }
}
