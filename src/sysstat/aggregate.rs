use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct AggregateSample {
    pub cpu: Option<f64>,
    pub mem_pct: Option<f64>,
    pub load1: Option<f64>,
    pub load5: Option<f64>,
    pub net_up_total: Option<u64>,
    pub net_down_total: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FinalizedAggregate {
    pub bucket_ts: i64,
    pub sample_count: u32,
    pub cpu_samples: u32,
    pub cpu_avg: Option<f64>,
    pub cpu_max: Option<f64>,
    pub mem_samples: u32,
    pub mem_pct_avg: Option<f64>,
    pub mem_pct_max: Option<f64>,
    pub load_1_avg: Option<f64>,
    pub load_5_avg: Option<f64>,
    pub net_up_bytes: Option<u64>,
    pub net_down_bytes: Option<u64>,
    pub network_duration_sec: u32,
    pub duration_sec: u32,
}

#[derive(Default)]
/// 按分钟聚合采样，网络流量使用相邻累计计数器的差值。
/// Aggregate samples by minute; network traffic is the difference between consecutive counters.
pub struct MinuteAggregator {
    bucket_ts: Option<i64>,
    first_observed_at: Option<i64>,
    last_observed_at: Option<i64>,
    prev_observed_at: Option<i64>,
    sample_count: u32,
    cpu_sum: f64,
    cpu_samples: u32,
    cpu_max: Option<f64>,
    mem_sum: f64,
    mem_samples: u32,
    mem_max: Option<f64>,
    load1_sum: f64,
    load5_sum: f64,
    load_samples: u32,
    net_up: u64,
    net_down: u64,
    prev_up: Option<u64>,
    prev_down: Option<u64>,
    network_samples: u32,
    network_duration_sec: u32,
}

impl MinuteAggregator {
    pub fn push(
        &mut self,
        observed_at: i64,
        sample: AggregateSample,
    ) -> Option<FinalizedAggregate> {
        let bucket = observed_at.div_euclid(60) * 60;
        let finalized = self
            .bucket_ts
            .filter(|current| *current != bucket)
            .and_then(|_| self.finish());
        if self.bucket_ts != Some(bucket) {
            // 跨分钟保留上一计数器，边界处的流量归入新分钟。
            // Retain prior counters across minutes; assign boundary traffic to the new minute.
            let prev_up = self.prev_up;
            let prev_down = self.prev_down;
            let prev_observed_at = self.prev_observed_at;
            *self = Self {
                bucket_ts: Some(bucket),
                prev_up,
                prev_down,
                prev_observed_at,
                ..Self::default()
            };
        }
        self.first_observed_at.get_or_insert(observed_at);
        self.last_observed_at = Some(observed_at);
        self.sample_count += 1;
        if let Some(value) = sample.cpu.filter(|v| v.is_finite()) {
            self.cpu_sum += value;
            self.cpu_samples += 1;
            self.cpu_max = Some(self.cpu_max.map_or(value, |max| max.max(value)));
        }
        if let Some(value) = sample.mem_pct.filter(|v| v.is_finite()) {
            self.mem_sum += value;
            self.mem_samples += 1;
            self.mem_max = Some(self.mem_max.map_or(value, |max| max.max(value)));
        }
        if let (Some(up), Some(down)) = (sample.net_up_total, sample.net_down_total) {
            if let (Some(prev_up), Some(prev_down), Some(prev_at)) =
                (self.prev_up, self.prev_down, self.prev_observed_at)
            {
                // 间隔过长不计入覆盖时间，计数器重置时差值归零。
                // Exclude long gaps from coverage; counter resets contribute zero.
                if observed_at > prev_at && observed_at - prev_at <= 60 {
                    self.net_up = self.net_up.saturating_add(up.saturating_sub(prev_up));
                    self.net_down = self.net_down.saturating_add(down.saturating_sub(prev_down));
                    self.network_duration_sec += (observed_at - prev_at) as u32;
                    self.network_samples += 1;
                }
            }
            self.prev_up = Some(up);
            self.prev_down = Some(down);
            self.prev_observed_at = Some(observed_at);
        } else {
            self.prev_up = None;
            self.prev_down = None;
            self.prev_observed_at = None;
        }
        if let (Some(load1), Some(load5)) = (sample.load1, sample.load5) {
            if load1.is_finite() && load5.is_finite() {
                self.load1_sum += load1;
                self.load5_sum += load5;
                self.load_samples += 1;
            }
        }
        finalized
    }

    pub fn finish(&mut self) -> Option<FinalizedAggregate> {
        let bucket_ts = self.bucket_ts.take()?;
        Some(FinalizedAggregate {
            bucket_ts,
            sample_count: self.sample_count,
            cpu_samples: self.cpu_samples,
            cpu_avg: mean(self.cpu_sum, self.cpu_samples),
            cpu_max: self.cpu_max,
            mem_samples: self.mem_samples,
            mem_pct_avg: mean(self.mem_sum, self.mem_samples),
            mem_pct_max: self.mem_max,
            load_1_avg: mean(self.load1_sum, self.load_samples),
            load_5_avg: mean(self.load5_sum, self.load_samples),
            net_up_bytes: (self.network_samples > 0).then_some(self.net_up),
            net_down_bytes: (self.network_samples > 0).then_some(self.net_down),
            network_duration_sec: self.network_duration_sec,
            duration_sec: self
                .last_observed_at
                .zip(self.first_observed_at)
                .map_or(0, |(last, first)| last.saturating_sub(first).max(0) as u32),
        })
    }
}

fn mean(sum: f64, count: u32) -> Option<f64> {
    (count > 0).then_some(sum / f64::from(count))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        cpu: Option<f64>,
        mem: Option<f64>,
        up: Option<u64>,
        down: Option<u64>,
    ) -> AggregateSample {
        AggregateSample {
            cpu,
            mem_pct: mem,
            load1: Some(1.0),
            load5: Some(2.0),
            net_up_total: up,
            net_down_total: down,
        }
    }

    #[test]
    fn computes_varied_sample_means_peaks_and_network_deltas() {
        let mut agg = MinuteAggregator::default();
        agg.push(120, sample(Some(10.0), Some(20.0), Some(100), Some(200)));
        agg.push(125, sample(Some(40.0), Some(50.0), Some(180), Some(260)));
        let result = agg
            .push(180, sample(Some(90.0), Some(90.0), Some(300), Some(400)))
            .unwrap();
        assert_eq!(result.bucket_ts, 120);
        assert_eq!(result.sample_count, 2);
        assert_eq!(result.cpu_samples, 2);
        assert_eq!(result.cpu_avg, Some(25.0));
        assert_eq!(result.cpu_max, Some(40.0));
        assert_eq!(result.mem_pct_avg, Some(35.0));
        assert_eq!(result.mem_pct_max, Some(50.0));
        assert_eq!(result.net_up_bytes, Some(80));
        assert_eq!(result.net_down_bytes, Some(60));
    }

    #[test]
    fn counter_resets_and_missing_readings_do_not_create_false_deltas() {
        let mut agg = MinuteAggregator::default();
        agg.push(60, sample(Some(5.0), None, Some(500), Some(500)));
        agg.push(65, sample(None, Some(30.0), Some(20), Some(20)));
        agg.push(70, sample(Some(15.0), None, Some(50), Some(80)));
        let result = agg.finish().unwrap();
        assert_eq!(result.sample_count, 3);
        assert_eq!(result.cpu_avg, Some(10.0));
        assert_eq!(result.cpu_max, Some(15.0));
        assert_eq!(result.mem_pct_avg, Some(30.0));
        assert_eq!(result.net_up_bytes, Some(30));
        assert_eq!(result.net_down_bytes, Some(60));
    }

    #[test]
    fn boundary_delta_is_kept_and_partial_duration_is_measured() {
        let mut agg = MinuteAggregator::default();
        agg.push(58, sample(Some(10.0), Some(20.0), Some(100), Some(200)));
        let first = agg
            .push(63, sample(Some(30.0), Some(40.0), Some(140), Some(260)))
            .unwrap();
        assert_eq!(first.duration_sec, 0);
        assert_eq!(first.net_up_bytes, None);
        agg.push(68, sample(Some(50.0), Some(60.0), Some(180), Some(300)));
        let second = agg.finish().unwrap();
        assert_eq!(second.duration_sec, 5);
        assert_eq!(second.net_up_bytes, Some(80));
        assert_eq!(second.net_down_bytes, Some(100));
        assert_eq!(second.network_duration_sec, 10);
    }

    #[test]
    fn long_sampling_gap_does_not_claim_network_coverage() {
        let mut agg = MinuteAggregator::default();
        agg.push(120, sample(Some(10.0), Some(20.0), Some(100), Some(200)));
        agg.push(181, sample(Some(30.0), Some(40.0), Some(180), Some(260)));
        let result = agg.finish().unwrap();
        assert_eq!(result.duration_sec, 0);
        assert_eq!(result.net_up_bytes, None);
        assert_eq!(result.net_down_bytes, None);
    }

    #[test]
    fn network_rate_uses_the_time_covered_by_accepted_counter_deltas() {
        let mut agg = MinuteAggregator::default();
        agg.push(175, sample(Some(1.0), Some(1.0), Some(0), Some(0)));
        let first = agg
            .push(180, sample(Some(1.0), Some(1.0), Some(500), Some(500)))
            .unwrap();
        assert_eq!(first.net_up_bytes, None);
        agg.push(185, sample(Some(1.0), Some(1.0), Some(1000), Some(1000)));
        let second = agg
            .push(240, sample(Some(1.0), Some(1.0), Some(6500), Some(6500)))
            .unwrap();
        assert_eq!(second.net_up_bytes, Some(1000));
        assert_eq!(second.network_duration_sec, 10);
        assert_eq!(
            second.net_up_bytes.unwrap() as f64 / f64::from(second.network_duration_sec),
            100.0
        );
        assert_eq!(second.duration_sec, 5);
        let third = agg.finish().unwrap();
        assert_eq!(third.net_up_bytes, Some(5500));
        assert_eq!(third.network_duration_sec, 55);
        assert_eq!(
            third.net_up_bytes.unwrap() as f64 / f64::from(third.network_duration_sec),
            100.0
        );
        assert_eq!(
            second.net_up_bytes.unwrap() + third.net_up_bytes.unwrap(),
            6500
        );
    }

    #[test]
    fn five_second_samples_queue_all_minutes_for_a_three_minute_report() {
        let mut agg = MinuteAggregator::default();
        let mut summaries = Vec::new();
        for at in (0..=180).step_by(5) {
            if let Some(summary) = agg.push(
                at,
                sample(
                    Some(at as f64),
                    Some(20.0),
                    Some(at as u64 * 100),
                    Some(at as u64 * 200),
                ),
            ) {
                summaries.push(summary);
            }
        }
        summaries.extend(agg.finish());
        assert_eq!(
            summaries.iter().map(|s| s.bucket_ts).collect::<Vec<_>>(),
            vec![0, 60, 120, 180]
        );
        assert_eq!(
            summaries
                .iter()
                .take(3)
                .map(|s| s.sample_count)
                .collect::<Vec<_>>(),
            vec![12, 12, 12]
        );
    }
}
