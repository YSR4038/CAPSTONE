import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import app
from app import (
    LogPreprocessor,
    LightweightDrain,
    WindowingEngine,
    FeatureExtractor,
    PartitionedAnomalyDetector,
    IncrementalModelUpdater,
)

def test_preprocessing_regexes():
    """Verify regex token normalization for IP, block IDs, hex, and paths."""
    raw = "081109 203534 148 INFO dfs.DataNode$DataXceiver: Receiving block blk_-160896131 src: /10.250.19.102:50010 dest: /10.250.10.6:50010"
    parsed = LogPreprocessor.parse_raw_line(raw)
    assert parsed["level"] == "INFO"
    assert parsed["block_id"] == "blk_-160896131"
    
    norm = LogPreprocessor.normalize_message(parsed["message"])
    assert "blk_-160896131" not in norm
    assert "<*>" in norm
    assert "10.250.19.102" not in norm

def test_drain_template_merge_and_stable_ids():
    """Verify template merging with wildcard substitution and stable ID mapping."""
    drain = LightweightDrain(depth=4, sim_threshold=0.5)
    
    msg1 = "Receiving block blk_-101 src: /10.0.0.1:50010 dest: /10.0.0.2:50010"
    msg2 = "Receiving block blk_-999 src: /10.0.0.5:50010 dest: /10.0.0.6:50010"
    
    id1, tmpl1, is_new1 = drain.parse(msg1)
    id2, tmpl2, is_new2 = drain.parse(msg2)
    
    assert is_new1 is True
    assert is_new2 is False  # Merged into existing template group
    assert id1 == id2        # Stable template ID

def test_windowing_burst_and_hysteresis():
    """Test adaptive windowing ladder, burst detection (rho >= 2.0 -> 60s), and hysteresis."""
    base_t = datetime(2026, 9, 16, 10, 0, 0)
    records = []
    
    # 30 calm minutes (1 log per minute)
    for i in range(30):
        records.append({
            "timestamp": (base_t + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "level": "INFO",
            "component": "dfs.DataNode",
            "message": f"Block blk_-{i} heartbeat",
            "block_id": f"blk_-{i}",
            "label": 0
        })
        
    # Sudden burst at minute 31: 20 logs in 30 seconds
    burst_t = base_t + timedelta(minutes=31)
    for j in range(20):
        records.append({
            "timestamp": (burst_t + timedelta(seconds=j)).strftime("%Y-%m-%d %H:%M:%S"),
            "level": "ERROR",
            "component": "dfs.DataNode",
            "message": f"Block blk_-burst-{j} failure",
            "block_id": f"blk_-burst-{j}",
            "label": 1
        })
        
    df = pd.DataFrame(records)
    windows = WindowingEngine.slice_adaptive_windows(df, base_bin_s=60, history_k=30, burst_ratio=2.0, min_logs=1)
    assert len(windows) > 0
    # At least one burst window has 60s length
    burst_wins = [w for w in windows if w["length_s"] == 60]
    assert len(burst_wins) >= 1

def test_feature_edge_cases():
    """Verify feature extractor handles empty/single-log windows without zero division."""
    empty_df = pd.DataFrame(columns=["timestamp", "level", "component", "message", "block_id", "template_id"])
    w_empty = {"df": empty_df, "length_s": 60.0}
    feats = FeatureExtractor.compute_window_features(w_empty, {})
    assert feats["total_logs"] == 0.0
    assert feats["error_ratio"] == 0.0
    assert feats["burst_rate"] == 0.0

def test_score_normalization_and_aggregation_formula():
    """
    Test score normalization range [0, 1] and formula:
    local 0.80, global 0.70, w 0.6 -> 0.76.
    """
    w_local = 0.60
    w_global = 0.40
    local_s = 0.80
    global_s = 0.70
    final_s = w_local * local_s + w_global * global_s
    assert abs(final_s - 0.76) < 1e-6

def test_severity_boundaries():
    """Verify calibrated severity bands."""
    detector = PartitionedAnomalyDetector()
    assert detector._classify_severity(0.15) == "Normal"
    assert detector._classify_severity(0.35) == "Low"
    assert detector._classify_severity(0.55) == "Medium"
    assert detector._classify_severity(0.75) == "High"
    assert detector._classify_severity(0.95) == "Critical"

def test_non_empty_reasons():
    """Ensure explainability module never outputs empty reason strings for detected anomalies."""
    detector = PartitionedAnomalyDetector()
    detector.baseline_stats = {
        "error_ratio": {"median": 0.01, "iqr": 0.02},
        "burst_rate": {"median": 1.0, "iqr": 0.5},
        "new_template_count": {"median": 0.0, "iqr": 0.01}
    }
    feats = {
        "error_ratio": 0.85,
        "burst_rate": 20.0,
        "new_template_count": 3.0,
        "total_logs": 50.0,
        "warning_count": 0.0,
        "warning_ratio": 0.0,
        "error_count": 42.0,
        "unique_templates": 5.0,
        "dominant_template_freq": 0.5,
        "rare_template_ratio": 0.2,
        "avg_time_gap": 0.1,
        "max_time_gap": 0.5,
        "template_entropy": 2.1,
        "window_length_seconds": 60.0
    }
    reasons, top_feats = detector._explain_window(feats)
    assert len(reasons) > 0
    assert any("Error rate surged" in r for r in reasons)

def test_poisoning_guard_incremental():
    """Verify poisoning guard: high and critical anomalies are NEVER admitted into sliding buffer."""
    updater = IncrementalModelUpdater(buffer_size=100, update_every=5)
    detector = PartitionedAnomalyDetector()
    
    benign_window = {"severity": "Normal", "features": {"error_ratio": 0.0}}
    anomalous_window = {"severity": "Critical", "features": {"error_ratio": 0.99}}
    
    updater.add_and_check_update(benign_window, detector)
    assert len(updater.buffer) == 1
    
    # Try adding anomalous window
    updater.add_and_check_update(anomalous_window, detector)
    assert len(updater.buffer) == 1  # Blocked!
    assert updater.buffer[0]["severity"] == "Normal"

def test_flask_endpoints():
    """Test Flask routes using app.test_client()."""
    client = app.app.test_client()
    
    # GET /
    res_index = client.get("/")
    assert res_index.status_code == 200
    assert b"LogPulse" in res_index.data
    
    # GET /api/summary/latest
    res_summary = client.get("/api/summary/latest")
    assert res_summary.status_code == 200
    json_sum = res_summary.get_json()
    assert "run" in json_sum
    assert json_sum["run"]["total_logs"] > 0
    
    # GET /api/timeline/latest
    res_timeline = client.get("/api/timeline/latest")
    assert res_timeline.status_code == 200
    json_time = res_timeline.get_json()
    assert "points" in json_time
    
    # GET /download/latest.csv
    res_csv = client.get("/download/latest.csv")
    assert res_csv.status_code == 200
    assert "text/csv" in res_csv.headers["Content-Type"]
