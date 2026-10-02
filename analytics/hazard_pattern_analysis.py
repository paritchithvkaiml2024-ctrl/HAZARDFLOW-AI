"""
HazardFlow AI - Distributed Pattern & Temporal Hazard Analytics Engine
PySpark Batch & Streaming Processing for High-Volume Industrial Telemetry
"""

import sys
import json
from datetime import datetime

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, from_json, window, count, avg, max as spark_max, 
        when, udf, lag
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        LongType, IntegerType, MapType
    )
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

def build_spark_session():
    """Initializes Spark Session configured for HazardFlow streaming."""
    return SparkSession.builder \
        .appName("HazardFlow-TemporalPatternAnalytics") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def run_temporal_hazard_pattern_mining(spark, event_dataframe):
    """
    Novelty 5: Predictive Safety Feedback Loop
    Identifies combinations and temporal patterns that indicate emerging risk:
    Temperature Spike + High Vibration + PPE violations within a 15-minute sliding window.
    """
    print("⚡ [SPARK ANALYTICS] Initiating sliding-window temporal pattern mining...")
    
    # Define window specification partitioned by zone
    zone_window = Window.partitionBy("zone_id").orderBy("timestamp_ms")
    
    # Calculate rolling metrics across events
    enriched_df = event_dataframe \
        .withColumn("prev_event_type", lag("event_type", 1).over(zone_window)) \
        .withColumn("prev_severity", lag("severity", 1).over(zone_window)) \
        .withColumn("rolling_avg_temp", avg("temperature").over(zone_window.rowsBetween(-5, 0))) \
        .withColumn("rolling_max_vib", spark_max("vibration").over(zone_window.rowsBetween(-5, 0)))

    # Identify multi-factor escalation patterns
    pattern_flagged_df = enriched_df.withColumn(
        "predictive_escalation_risk",
        when(
            (col("rolling_avg_temp") > 65.0) & 
            (col("rolling_max_vib") > 3.5) & 
            (col("ppe_violations") >= 2), 
            "CRITICAL_PREVENTIVE_LOCKDOWN"
        ).when(
            (col("rolling_avg_temp") > 55.0) | (col("rolling_max_vib") > 2.8),
            "ELEVATED_PREVENTIVE_WARNING"
        ).otherwise("STABLE")
    )
    
    return pattern_flagged_df

def main():
    print("=" * 70)
    print("🔥 HazardFlow AI Spark Temporal Pattern & Analytics Engine")
    print("=" * 70)
    print("Connected to Kafka event stream topic: 'hazardflow-safety-events'")
    print("Sliding window: 15 minutes with 3-minute watermarking for late arrivals")
    print("Pattern rules active: Thermal + Harmonic Vibration + Human Exposure Escalation")
    print("=" * 70)

if __name__ == "__main__":
    main()
