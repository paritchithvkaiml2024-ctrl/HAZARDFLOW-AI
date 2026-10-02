use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RiskLevel {
    Low = 1,
    Medium = 2,
    High = 3,
    Critical = 4,
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskLevel::Low => "LOW",
            RiskLevel::Medium => "MEDIUM",
            RiskLevel::High => "HIGH",
            RiskLevel::Critical => "CRITICAL",
        }
    }

    pub fn priority_weight(&self) -> u32 {
        match self {
            RiskLevel::Low => 10,
            RiskLevel::Medium => 50,
            RiskLevel::High => 200,
            RiskLevel::Critical => 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyRecord {
    pub stage: String,
    pub latency_us: u64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyAssessment {
    pub is_anomaly: bool,
    pub confidence: f64,
    pub anomaly_score: f64,
    pub recommended_actions: Vec<String>,
}

pub fn compute_cumulative_zone_risk(
    ppe_violations: u32,
    machine_anomalies: u32,
    temp_celsius: f64,
    vib_g: f64,
    personnel_count: u32,
) -> (f64, RiskLevel) {
    let mut score = 0.0;

    // Environmental weighting
    if temp_celsius > 65.0 {
        score += (temp_celsius - 65.0) * 1.5;
    }
    if vib_g > 3.5 {
        score += (vib_g - 3.5) * 12.0;
    }

    // Direct violations
    score += (ppe_violations as f64) * 15.0;
    score += (machine_anomalies as f64) * 25.0;

    // Personnel exposure multiplier
    if personnel_count > 0 && score > 20.0 {
        score *= 1.0 + (personnel_count as f64 * 0.08);
    }

    let level = if score >= 80.0 {
        RiskLevel::Critical
    } else if score >= 45.0 {
        RiskLevel::High
    } else if score >= 20.0 {
        RiskLevel::Medium
    } else {
        RiskLevel::Low
    };

    (score.min(100.0), level)
}
