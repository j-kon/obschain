use chrono::Utc;
use obschain_core::{
    BaselineDistribution, BaselineMetric, BaselineQuality, BaselineRun, BaselineRunStatus,
    EvaluationMode, EventMetricExtractor, EventRarityContext, EventRarityResult, EventType,
    HalvingEpoch, ImpactBreakdown, ImpactComponent, ImpactComponentDefinition,
    ImpactModelDefinition, ImpactUnavailableReason, MetricRegistry, MetricValue,
    PercentileEvaluationDecision, PercentileMethod, QuantileDistribution, RarityBand,
    RarityDirection,
};
use uuid::Uuid;

pub const ALGORITHM_VERSION_V1: &str = "obschain-baseline-v1";
pub const ALGORITHM_VERSION_V2: &str = "obschain-baseline-v2";
pub const DEFAULT_ALGORITHM_VERSION: &str = ALGORITHM_VERSION_V2;
pub const DEFAULT_IMPACT_MODEL_VERSION: &str = "v1";
pub const DEFAULT_MIN_SAMPLE_SIZE: u64 = 100;

/// Statistical calculator for distributions, quantiles, and empirical CDF ranks.
pub struct BaselineCalculator;

impl BaselineCalculator {
    /// Computes discrete quantiles (percentile_disc) and summary statistics from a sorted slice of MetricValues.
    ///
    /// # Quantile Semantics:
    /// For a sorted list of N samples, at rank q in [0, 1], we select:
    /// index = clamp(ceil(q * N) - 1, 0, N - 1)
    /// This returns an observed historical value, preserving exact integer satoshi and count precision.
    pub fn compute_distribution(
        values: &[MetricValue],
        candidate_count: u64,
    ) -> Option<QuantileDistribution> {
        if values.is_empty() {
            return None;
        }

        let mut sorted = values.to_vec();
        sorted.sort();

        let n = sorted.len();
        let sample_count = n as u64;
        let missing_count = candidate_count.saturating_sub(sample_count);
        let coverage_ratio = if candidate_count > 0 {
            (sample_count as f64) / (candidate_count as f64)
        } else {
            1.0
        };

        let minimum = sorted[0];
        let maximum = sorted[n - 1];

        // Discrete quantile lookup: index = ceil(q * N) - 1
        let quantile_at = |q: f64| -> MetricValue {
            let rank = (q * (n as f64)).ceil() as usize;
            let idx = if rank == 0 { 0 } else { (rank - 1).min(n - 1) };
            sorted[idx]
        };

        let p50 = quantile_at(0.50);
        let p75 = quantile_at(0.75);
        let p90 = quantile_at(0.90);
        let p95 = quantile_at(0.95);
        let p99 = quantile_at(0.99);
        let p999 = quantile_at(0.999);

        // Arithmetic mean
        let sum_f64: f64 = sorted.iter().map(|v| v.to_f64()).sum();
        let mean = sum_f64 / (n as f64);

        let quality = BaselineQuality::evaluate(sample_count, coverage_ratio);

        Some(QuantileDistribution {
            sample_count,
            candidate_count,
            missing_count,
            coverage_ratio,
            minimum,
            maximum,
            mean,
            p50,
            p75,
            p90,
            p95,
            p99,
            p999,
            quality,
        })
    }

    /// Evaluates the empirical percentile rank of a value against sorted historical samples.
    ///
    /// # Exact Percentile Rank & Tie Semantics:
    /// - For HigherIsRarer:
    ///   count_le = count of samples with value <= x
    ///   tail_count = count of samples with value >= x
    ///   percentile = (count_le / N) * 100.0
    /// - For LowerIsRarer:
    ///   count_ge = count of samples with value >= x
    ///   tail_count = count of samples with value <= x
    ///   percentile = (count_ge / N) * 100.0
    ///
    /// Ties are treated inclusively in both count_le and tail_count.
    pub fn rank_value(
        sorted_samples: &[MetricValue],
        value: MetricValue,
        direction: RarityDirection,
    ) -> (f64, u64) {
        if sorted_samples.is_empty() {
            return (0.0, 0);
        }

        let n = sorted_samples.len();
        match direction {
            RarityDirection::HigherIsRarer | RarityDirection::TwoSided => {
                // Number of elements <= value
                let count_le = sorted_samples.iter().filter(|&&v| v <= value).count();
                // Number of elements >= value (tail count)
                let tail_count = sorted_samples.iter().filter(|&&v| v >= value).count() as u64;
                let percentile = ((count_le as f64) / (n as f64)) * 100.0;
                (percentile, tail_count)
            }
            RarityDirection::LowerIsRarer => {
                // Number of elements >= value
                let count_ge = sorted_samples.iter().filter(|&&v| v >= value).count();
                // Number of elements <= value (tail count)
                let tail_count = sorted_samples.iter().filter(|&&v| v <= value).count() as u64;
                let percentile = ((count_ge as f64) / (n as f64)) * 100.0;
                (percentile, tail_count)
            }
        }
    }

    /// Fast percentile approximation using precomputed distribution quantiles
    /// when raw sample lists are not in memory.
    pub fn rank_from_distribution(
        dist: &BaselineDistribution,
        value: MetricValue,
        direction: RarityDirection,
    ) -> (f64, u64) {
        let n = dist.sample_count;
        if n == 0 {
            return (0.0, 0);
        }

        let val_f = value.to_f64();
        let min_f = dist.minimum.to_f64();
        let max_f = dist.maximum.to_f64();

        if val_f <= min_f {
            let tail = n;
            let p = if direction == RarityDirection::LowerIsRarer {
                100.0
            } else {
                0.0
            };
            return (p, tail);
        }
        if val_f >= max_f {
            let tail = 1;
            let p = if direction == RarityDirection::LowerIsRarer {
                0.0
            } else {
                100.0
            };
            return (p, tail);
        }

        // Piecewise linear interpolation between standard quantiles
        let quantiles = [
            (0.0, min_f),
            (50.0, dist.p50.to_f64()),
            (75.0, dist.p75.to_f64()),
            (90.0, dist.p90.to_f64()),
            (95.0, dist.p95.to_f64()),
            (99.0, dist.p99.to_f64()),
            (99.9, dist.p999.to_f64()),
            (100.0, max_f),
        ];

        let mut percentile = 50.0;
        for i in 0..(quantiles.len() - 1) {
            let (q_low, v_low) = quantiles[i];
            let (q_high, v_high) = quantiles[i + 1];

            if val_f >= v_low && val_f <= v_high {
                if (v_high - v_low).abs() < f64::EPSILON {
                    percentile = q_high;
                } else {
                    let fraction = (val_f - v_low) / (v_high - v_low);
                    percentile = q_low + fraction * (q_high - q_low);
                }
                break;
            }
        }

        let tail = (((100.0 - percentile) / 100.0) * (n as f64))
            .ceil()
            .max(1.0) as u64;
        (percentile, tail)
    }
}

/// Explainable impact scoring engine.
/// Computes normalized, component-by-component points (0..100)
/// without black-box machine learning or arbitrary clamping.
pub struct ImpactCalculator;

impl ImpactCalculator {
    /// Returns the official event-type-specific impact model definition.
    pub fn model_for_event_type(event_type: EventType) -> Option<ImpactModelDefinition> {
        match event_type {
            EventType::LargeTransfer => Some(ImpactModelDefinition {
                id: "obschain-impact-large-transfer-v1",
                event_type,
                version: "v1",
                components: vec![
                    ImpactComponentDefinition {
                        name: "Value rarity",
                        metric: BaselineMetric::ValueSats,
                        weight: 60.0,
                    },
                    ImpactComponentDefinition {
                        name: "Input count structure",
                        metric: BaselineMetric::InputCount,
                        weight: 15.0,
                    },
                    ImpactComponentDefinition {
                        name: "Output count structure",
                        metric: BaselineMetric::OutputCount,
                        weight: 15.0,
                    },
                    ImpactComponentDefinition {
                        name: "Transaction size anomaly",
                        metric: BaselineMetric::Vsize,
                        weight: 10.0,
                    },
                ],
            }),
            EventType::DormantCoinsMoved => Some(ImpactModelDefinition {
                id: "obschain-impact-dormant-coins-v1",
                event_type,
                version: "v1",
                components: vec![
                    ImpactComponentDefinition {
                        name: "Dormant value rarity",
                        metric: BaselineMetric::DormantValueSats,
                        weight: 35.0,
                    },
                    ImpactComponentDefinition {
                        name: "Coin-age rarity",
                        metric: BaselineMetric::OldestInputAgeDays,
                        weight: 30.0,
                    },
                    ImpactComponentDefinition {
                        name: "Coin-age-destroyed rarity",
                        metric: BaselineMetric::CoinAgeDestroyedSatoshiDays,
                        weight: 25.0,
                    },
                    ImpactComponentDefinition {
                        name: "Input count structure",
                        metric: BaselineMetric::InputCount,
                        weight: 10.0,
                    },
                ],
            }),
            EventType::LongBlockInterval => Some(ImpactModelDefinition {
                id: "obschain-impact-long-block-interval-v1",
                event_type,
                version: "v1",
                components: vec![ImpactComponentDefinition {
                    name: "Network interval rarity",
                    metric: BaselineMetric::IntervalSeconds,
                    weight: 100.0,
                }],
            }),
            EventType::Consolidation => Some(ImpactModelDefinition {
                id: "obschain-impact-consolidation-v1",
                event_type,
                version: "v1",
                components: vec![
                    ImpactComponentDefinition {
                        name: "Input count structure",
                        metric: BaselineMetric::InputCount,
                        weight: 40.0,
                    },
                    ImpactComponentDefinition {
                        name: "Consolidation ratio rarity",
                        metric: BaselineMetric::ConsolidationRatio,
                        weight: 30.0,
                    },
                    ImpactComponentDefinition {
                        name: "Consolidation value rarity",
                        metric: BaselineMetric::ValueSats,
                        weight: 20.0,
                    },
                    ImpactComponentDefinition {
                        name: "Output count structure",
                        metric: BaselineMetric::OutputCount,
                        weight: 10.0,
                    },
                ],
            }),
            EventType::FanOut => Some(ImpactModelDefinition {
                id: "obschain-impact-fan-out-v1",
                event_type,
                version: "v1",
                components: vec![
                    ImpactComponentDefinition {
                        name: "Output count structure",
                        metric: BaselineMetric::OutputCount,
                        weight: 40.0,
                    },
                    ImpactComponentDefinition {
                        name: "Distributed volume rarity",
                        metric: BaselineMetric::DistributedValueSats,
                        weight: 35.0,
                    },
                    ImpactComponentDefinition {
                        name: "Median output size anomaly",
                        metric: BaselineMetric::MedianOutputSats,
                        weight: 25.0,
                    },
                ],
            }),
            EventType::ExtremeFee => Some(ImpactModelDefinition {
                id: "obschain-impact-extreme-fee-v1",
                event_type,
                version: "v1",
                components: vec![
                    ImpactComponentDefinition {
                        name: "Fee rate rarity",
                        metric: BaselineMetric::FeeRateSatVb,
                        weight: 60.0,
                    },
                    ImpactComponentDefinition {
                        name: "Absolute fee rarity",
                        metric: BaselineMetric::FeeSats,
                        weight: 40.0,
                    },
                ],
            }),
            _ => None,
        }
    }

    /// Computes explainable impact breakdown for an event given its evaluated rarity metrics.
    pub fn calculate_impact(
        event_type: EventType,
        rarity_results: &[EventRarityResult],
        model_version: Option<&str>,
    ) -> ImpactBreakdown {
        let model =
            Self::model_for_event_type(event_type).unwrap_or_else(|| ImpactModelDefinition {
                id: "obschain-impact-generic-v1",
                event_type,
                version: "v1",
                components: Vec::new(),
            });

        let version = model_version.unwrap_or(model.version);
        let mut components = Vec::new();
        let expected_count = model.components.len();
        let mut available_count = 0usize;
        let mut has_insufficient_samples = false;

        for comp_def in &model.components {
            if let Some(res) = rarity_results.iter().find(|r| r.metric == comp_def.metric) {
                available_count += 1;
                if res.population_size < DEFAULT_MIN_SAMPLE_SIZE || res.percentile.is_none() {
                    has_insufficient_samples = true;
                }

                let points_awarded = if let Some(p) = res.percentile {
                    if p >= 50.0 {
                        let normalized = (p - 50.0) / 50.0;
                        comp_def.weight * normalized
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                components.push(ImpactComponent {
                    component_name: comp_def.name.to_string(),
                    metric: comp_def.metric,
                    raw_value: res.value.to_numeric_string(),
                    percentile: res.percentile,
                    weight: comp_def.weight,
                    points_awarded: (points_awarded * 100.0).round() / 100.0,
                    population_size: res.population_size,
                });
            }
        }

        let model_coverage = if expected_count > 0 {
            (available_count as f64) / (expected_count as f64)
        } else {
            0.0
        };

        // Score is available only if coverage >= 50% and sample size >= minimum and available_count > 0
        let (total_score, unavailable_reason) = if available_count == 0 {
            (None, Some(ImpactUnavailableReason::RarityUnavailable))
        } else if has_insufficient_samples {
            (None, Some(ImpactUnavailableReason::InsufficientBaseline))
        } else if model_coverage < 0.50 {
            (
                None,
                Some(ImpactUnavailableReason::InsufficientComponentCoverage),
            )
        } else {
            let raw_points: f64 = components.iter().map(|c| c.points_awarded).sum();
            // Mathematical proof: each points_awarded <= weight, and sum of weights == 100.0.
            // Therefore, raw_points <= 100.0 by construction without structural clamping.
            let clamped = if raw_points > 100.00000001 {
                100.0
            } else {
                raw_points
            };
            (Some((clamped * 100.0).round() / 100.0), None)
        };

        ImpactBreakdown {
            model_id: model.id.to_string(),
            model_version: version.to_string(),
            event_type,
            status: "EXPERIMENTAL".to_string(),
            total_score,
            max_possible_points: 100.0,
            model_coverage,
            coverage_ratio: model_coverage,
            unavailable_reason,
            components,
        }
    }
}

/// Baseline service orchestrating statistical calculations across canonical events.
pub struct BaselineEngine;

impl BaselineEngine {
    /// Builds a completed BaselineRun record from window configuration.
    pub fn create_run_record(
        network: &str,
        start_height: u64,
        end_height: u64,
        algorithm_version: &str,
    ) -> BaselineRun {
        BaselineRun {
            id: Uuid::new_v4(),
            network: network.to_string(),
            start_height,
            end_height,
            started_at: Utc::now(),
            completed_at: None,
            status: BaselineRunStatus::Running,
            algorithm_version: algorithm_version.to_string(),
            canonical_event_count: 0,
            error_message: None,
            metadata: serde_json::json!({
                "halving_epoch_start": HalvingEpoch::epoch_from_height(start_height),
                "halving_epoch_end": HalvingEpoch::epoch_from_height(end_height),
            }),
            created_at: Utc::now(),
        }
    }

    /// Computes baseline distributions for all registered metrics across the provided canonical events.
    pub fn generate_distributions(
        baseline_run_id: Uuid,
        events: &[obschain_core::ChainEvent],
        target_event_types: Option<&[EventType]>,
    ) -> Vec<BaselineDistribution> {
        let all_types = MetricRegistry::supported_event_types();
        let types_to_process = target_event_types.unwrap_or(&all_types);
        let mut distributions = Vec::new();

        for &event_type in types_to_process {
            let events_of_type: Vec<&obschain_core::ChainEvent> = events
                .iter()
                .filter(|e| e.event_type == event_type)
                .collect();
            let candidate_count = events_of_type.len() as u64;

            let metric_defs = MetricRegistry::metrics_for_event_type(event_type);
            for def in metric_defs {
                let mut samples = Vec::new();
                for ev in &events_of_type {
                    if let Some(val) = EventMetricExtractor::extract_metric(ev, def.metric) {
                        samples.push(val);
                    }
                }

                if let Some(qdist) =
                    BaselineCalculator::compute_distribution(&samples, candidate_count)
                {
                    distributions.push(BaselineDistribution {
                        id: Uuid::new_v4(),
                        baseline_run_id,
                        event_type,
                        metric: def.metric,
                        unit: def.unit,
                        sample_count: qdist.sample_count,
                        candidate_count: qdist.candidate_count,
                        missing_count: qdist.missing_count,
                        coverage_ratio: qdist.coverage_ratio,
                        minimum: qdist.minimum,
                        maximum: qdist.maximum,
                        mean: qdist.mean,
                        p50: qdist.p50,
                        p75: qdist.p75,
                        p90: qdist.p90,
                        p95: qdist.p95,
                        p99: qdist.p99,
                        p999: qdist.p999,
                        quality: qdist.quality,
                        samples_json: None,
                        created_at: Utc::now(),
                    });
                }
            }
        }

        distributions
    }

    /// Evaluates an event against a collection of precomputed baseline distributions with optional exact ranks.
    pub fn evaluate_event_rarity_with_exact_ranks(
        event: &obschain_core::ChainEvent,
        baseline_run: &BaselineRun,
        distributions: &[BaselineDistribution],
        exact_ranks: &std::collections::HashMap<BaselineMetric, (f64, u64, u64)>,
        min_sample_size: u64,
    ) -> EventRarityContext {
        let is_retrospective = event
            .block_height
            .map(|h| h <= baseline_run.end_height)
            .unwrap_or(true);
        let evaluation_mode = if is_retrospective {
            EvaluationMode::Retrospective
        } else {
            EvaluationMode::PointInTime
        };

        let defs = MetricRegistry::metrics_for_event_type(event.event_type);
        let mut results = Vec::new();

        for def in defs {
            let Some(val) = EventMetricExtractor::extract_metric(event, def.metric) else {
                continue;
            };

            let exact_rank = exact_ranks.get(&def.metric).copied();
            let dist_opt = distributions
                .iter()
                .find(|d| d.event_type == event.event_type && d.metric == def.metric);

            let decision =
                PercentileEvaluationDecision::decide(exact_rank, dist_opt, min_sample_size);

            let (percentile_opt, method, estimated, tail_count, pop_size, band, quality) =
                match decision {
                    PercentileEvaluationDecision::Exact => {
                        let (p, tail, pop) = exact_rank.expect("decision Exact implies exact_rank");
                        let b = RarityBand::from_percentile(p, pop, min_sample_size);
                        let qual = dist_opt
                            .map(|d| d.quality)
                            .unwrap_or_else(|| BaselineQuality::evaluate(pop, 1.0));
                        (
                            Some(p),
                            Some(PercentileMethod::ExactEmpiricalCdf),
                            false,
                            Some(tail),
                            pop,
                            b,
                            Some(qual),
                        )
                    }
                    PercentileEvaluationDecision::Estimated => {
                        let dist = dist_opt.expect("decision Estimated implies distribution");
                        let (p, tail) =
                            BaselineCalculator::rank_from_distribution(dist, val, def.direction);
                        let b = RarityBand::from_percentile(p, dist.sample_count, min_sample_size);
                        (
                            Some(p),
                            Some(PercentileMethod::QuantileInterpolationEstimate),
                            true,
                            Some(tail),
                            dist.sample_count,
                            b,
                            Some(dist.quality),
                        )
                    }
                    PercentileEvaluationDecision::InsufficientData => {
                        let pop = exact_rank
                            .map(|(_, _, pop)| pop)
                            .or_else(|| dist_opt.map(|d| d.sample_count))
                            .unwrap_or(0);
                        let qual = dist_opt
                            .map(|d| d.quality)
                            .unwrap_or(BaselineQuality::Insufficient);
                        (
                            None,
                            None,
                            false,
                            None,
                            pop,
                            RarityBand::InsufficientData,
                            Some(qual),
                        )
                    }
                };

            results.push((
                def.is_primary,
                EventRarityResult {
                    event_id: event.id,
                    baseline_run_id: baseline_run.id,
                    event_type: event.event_type,
                    metric: def.metric,
                    value: val,
                    percentile: percentile_opt,
                    percentile_method: method,
                    estimated,
                    rarity_band: band,
                    population_size: pop_size,
                    tail_count,
                    evaluation_mode,
                    baseline_quality: quality,
                },
            ));
        }

        // Separate primary from secondary
        let primary = results
            .iter()
            .find(|(is_prim, _)| *is_prim)
            .map(|(_, res)| res.clone())
            .unwrap_or_else(|| {
                let fallback_metric = MetricRegistry::primary_metric(event.event_type)
                    .map(|d| d.metric)
                    .unwrap_or(BaselineMetric::ValueSats);
                EventRarityResult {
                    event_id: event.id,
                    baseline_run_id: baseline_run.id,
                    event_type: event.event_type,
                    metric: fallback_metric,
                    value: MetricValue::U64(0),
                    percentile: None,
                    percentile_method: None,
                    estimated: false,
                    rarity_band: RarityBand::InsufficientData,
                    population_size: 0,
                    tail_count: None,
                    evaluation_mode,
                    baseline_quality: Some(BaselineQuality::Insufficient),
                }
            });

        let secondary: Vec<EventRarityResult> = results
            .into_iter()
            .filter(|(is_prim, _)| !*is_prim)
            .map(|(_, res)| res)
            .collect();

        let mut all_for_impact = vec![primary.clone()];
        all_for_impact.extend(secondary.clone());

        let impact = Some(ImpactCalculator::calculate_impact(
            event.event_type,
            &all_for_impact,
            Some(&baseline_run.algorithm_version),
        ));

        EventRarityContext {
            baseline_id: baseline_run.id,
            baseline_version: baseline_run.algorithm_version.clone(),
            evaluation_mode,
            primary,
            secondary,
            impact,
        }
    }

    /// Evaluates an event against a collection of precomputed baseline distributions.
    pub fn evaluate_event_rarity(
        event: &obschain_core::ChainEvent,
        baseline_run: &BaselineRun,
        distributions: &[BaselineDistribution],
        min_sample_size: u64,
    ) -> EventRarityContext {
        let empty_exact = std::collections::HashMap::new();
        Self::evaluate_event_rarity_with_exact_ranks(
            event,
            baseline_run,
            distributions,
            &empty_exact,
            min_sample_size,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_quantiles_exact_deterministic_fixture() {
        // Deterministic fixture samples: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        let samples: Vec<MetricValue> = (1..=10).map(|i| MetricValue::U64(i * 10)).collect();
        let dist = BaselineCalculator::compute_distribution(&samples, 10).unwrap();

        assert_eq!(dist.sample_count, 10);
        assert_eq!(dist.minimum, MetricValue::U64(10));
        assert_eq!(dist.maximum, MetricValue::U64(100));
        assert_eq!(dist.mean, 55.0);

        // Discrete quantile lookup:
        // p50: ceil(0.50 * 10) - 1 = idx 4 -> 50
        assert_eq!(dist.p50, MetricValue::U64(50));
        // p75: ceil(0.75 * 10) - 1 = idx 7 -> 80
        assert_eq!(dist.p75, MetricValue::U64(80));
        // p90: ceil(0.90 * 10) - 1 = idx 8 -> 90
        assert_eq!(dist.p90, MetricValue::U64(90));
        // p95: ceil(0.95 * 10) - 1 = idx 9 -> 100
        assert_eq!(dist.p95, MetricValue::U64(100));
        // p99: ceil(0.99 * 10) - 1 = idx 9 -> 100
        assert_eq!(dist.p99, MetricValue::U64(100));
    }

    #[test]
    fn test_percentile_rank_empirical_cdf_and_ties() {
        // Historical values: 10, 20, 30, 40, 50
        let samples = vec![
            MetricValue::U64(10),
            MetricValue::U64(20),
            MetricValue::U64(30),
            MetricValue::U64(40),
            MetricValue::U64(50),
        ];

        // Current value: 40
        // count(<= 40) = 4, total = 5 -> percentile = 80.0%
        let (p40, tail40) = BaselineCalculator::rank_value(
            &samples,
            MetricValue::U64(40),
            RarityDirection::HigherIsRarer,
        );
        assert_eq!(p40, 80.0);
        assert_eq!(tail40, 2); // 40, 50

        // Test duplicate values / ties: 10, 10, 10, 20, 20, 30
        let tied_samples = vec![
            MetricValue::U64(10),
            MetricValue::U64(10),
            MetricValue::U64(10),
            MetricValue::U64(20),
            MetricValue::U64(20),
            MetricValue::U64(30),
        ];

        // 10: count(<=10) = 3 -> 3/6 = 50.0%
        let (p10, tail10) = BaselineCalculator::rank_value(
            &tied_samples,
            MetricValue::U64(10),
            RarityDirection::HigherIsRarer,
        );
        assert_eq!(p10, 50.0);
        assert_eq!(tail10, 6);

        // 20: count(<=20) = 5 -> 5/6 = 83.333...%
        let (p20, tail20) = BaselineCalculator::rank_value(
            &tied_samples,
            MetricValue::U64(20),
            RarityDirection::HigherIsRarer,
        );
        assert!((p20 - 83.333).abs() < 0.01);
        assert_eq!(tail20, 3); // 20, 20, 30

        // 30: count(<=30) = 6 -> 6/6 = 100.0%
        let (p30, tail30) = BaselineCalculator::rank_value(
            &tied_samples,
            MetricValue::U64(30),
            RarityDirection::HigherIsRarer,
        );
        assert_eq!(p30, 100.0);
        assert_eq!(tail30, 1); // 30
    }

    #[test]
    fn test_large_integer_and_u128_no_overflow() {
        let max_btc_sats = MetricValue::U64(2_100_000_000_000_000);
        let huge_cad = MetricValue::U128(987_654_321_000_000_000_000u128);

        let samples = vec![max_btc_sats, max_btc_sats];
        let dist = BaselineCalculator::compute_distribution(&samples, 2).unwrap();
        assert_eq!(dist.p50, max_btc_sats);

        let cad_samples = vec![huge_cad, huge_cad];
        let cad_dist = BaselineCalculator::compute_distribution(&cad_samples, 2).unwrap();
        assert_eq!(cad_dist.p99, huge_cad);
    }

    #[test]
    fn test_explainable_impact_scoring_formula() {
        let event_id = Uuid::new_v4();
        let baseline_id = Uuid::new_v4();

        let primary_rarity = EventRarityResult {
            event_id,
            baseline_run_id: baseline_id,
            event_type: EventType::LargeTransfer,
            metric: BaselineMetric::ValueSats,
            value: MetricValue::U64(10_000_000_000),
            percentile: Some(99.94),
            percentile_method: Some(PercentileMethod::ExactEmpiricalCdf),
            estimated: false,
            rarity_band: RarityBand::Extreme,
            population_size: 18421,
            tail_count: Some(11),
            evaluation_mode: EvaluationMode::Retrospective,
            baseline_quality: Some(BaselineQuality::High),
        };

        let secondary_rarity = EventRarityResult {
            event_id,
            baseline_run_id: baseline_id,
            event_type: EventType::LargeTransfer,
            metric: BaselineMetric::OutputCount,
            value: MetricValue::U64(50),
            percentile: Some(95.0),
            percentile_method: Some(PercentileMethod::ExactEmpiricalCdf),
            estimated: false,
            rarity_band: RarityBand::Unusual,
            population_size: 18421,
            tail_count: Some(920),
            evaluation_mode: EvaluationMode::Retrospective,
            baseline_quality: Some(BaselineQuality::High),
        };

        let breakdown = ImpactCalculator::calculate_impact(
            EventType::LargeTransfer,
            &[primary_rarity, secondary_rarity],
            None,
        );

        assert_eq!(breakdown.status, "EXPERIMENTAL");
        assert_eq!(breakdown.model_id, "obschain-impact-large-transfer-v1");
        assert_eq!(breakdown.model_coverage, 0.50);
        assert_eq!(breakdown.components.len(), 2);

        // Value rarity (weight 60.0, 99.94th percentile):
        // normalized = (99.94 - 50.0)/50.0 = 49.94/50.0 = 0.9988 -> points = 59.93
        let comp_val = &breakdown.components[0];
        assert_eq!(comp_val.component_name, "Value rarity");
        assert_eq!(comp_val.weight, 60.0);
        assert!((comp_val.points_awarded - 59.93).abs() < 0.05);

        // Output count structure (weight 15.0, 95.0th percentile):
        // normalized = (95.0 - 50.0)/50.0 = 45.0/50.0 = 0.90 -> points = 13.5
        let comp_struct = &breakdown.components[1];
        assert_eq!(comp_struct.component_name, "Output count structure");
        assert_eq!(comp_struct.weight, 15.0);
        assert!((comp_struct.points_awarded - 13.5).abs() < 0.05);

        // Total score: 59.93 + 13.5 = 73.43
        assert!(breakdown.total_score.is_some());
        let score = breakdown.total_score.unwrap();
        assert!((score - 73.43).abs() < 0.1);
    }
}
