# LogPulse: Lightweight Distributed Log Anomaly Detection

**Based on:** Wu et al., *"Distributed log anomaly detection method based on improved HDBSCAN"*, *Discover Computing* (2026) 29:393.

---

## 1. Project Overview & Research Context

Modern distributed clusters (such as Hadoop HDFS, OpenStack, and supercomputing systems like BGL) produce vast volumes of streaming log data. Detecting anomalous operational behavior (such as block corruptions, connection timeouts, or resource overloads) is critical to preventing outages.

The reference paper (Wu et al., 2026) proposed an end-to-end framework using:
- Distributed Drain with dynamic parse tree expansion on Apache Spark.
- Borůvka Minimum Spanning Tree (MST) and RTree spatial indexing.
- Improved HDBSCAN with Gaussian-kernel cross-partition fusion and GLOSH anomaly scoring.
- Evaluation on a dedicated Spark cluster (1 master + 3 worker nodes, 172 GB RAM).

The authors explicitly identified **adaptive dynamic windowing** and **online incremental learning** as vital future directions.

### Our Lightweight Innovation
**LogPulse** delivers a lightweight, production-grade, single-machine distributed alternative engineered for CPU execution (under 8 GB RAM) without requiring an Apache Spark cluster or GPU infrastructure:
1. **Lightweight Drain Parser**: Prefix tree with wildcard substitution (`<*>`), stable JSON template library persistence, and parallel template synchronization.
2. **Adaptive Dynamic Windowing**: Solves the paper's fixed-window limitation by dynamically adjusting the temporal receptive field (1 min for bursts, 5 min for rising rates, 10 min for baseline operations) with cooldown hysteresis.
3. **Partitioned Parallel Processing**: Uses Python ProcessPool multiprocessing across data partitions.
4. **Isolation Forest with Score Normalization**: Replaces $O(n^2)$ clustering bottlenecks with Isolation Forest, calibrating raw anomaly scores into $[0, 1]$ via 1st and 99th train percentiles.
5. **Global Frequency Rarity Fusion**: Fuses local partition density with global template inverse log frequency:  
   $$S_{final} = 0.6 \times S_{local} + 0.4 \times S_{global}$$
6. **Explainability Engine**: Computes robust z-scores against normal baseline median and IQR to generate top-3 contributing features and human-readable root causes.
7. **Incremental Baseline Updating**: Continuously refits baseline statistics and models with a poisoning guard (only Normal/Low severity windows are admitted into the update buffer).
8. **Futuristic Cosmos UI Dashboard**: Real-time interactive telemetry monitor with Deep Space Black & Cosmos Blue aesthetic, live SVG timeline, and drill-down triage modals.

---

## 2. System Architecture

```
+-------------------------------------------------------------------------+
|                              RAW LOG STREAM                             |
|       (HDFS Block Sessions / Timestamped Streaming Ingestion)           |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                  STAGE 1: PREPROCESSING & LIGHTWEIGHT DRAIN             |
|   - Regex Token Normalization: blk_<*>, <IP:PORT>, Hex, Numeric Paths   |
|   - Prefix Tree Token Matching & Dynamic Template Expansion             |
|   - Output: Structured Fields + Stable Template ID (E001, E002, ...)    |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                     STAGE 2: ADAPTIVE WINDOWING ENGINE                  |
|   - Base Bins (B = 60s), Rate r_t, Baseline Median m_t (K = 30 bins)    |
|   - Burst Ratio: rho_t = r_t / max(m_t, 1)                              |
|   - Ladder:                                                             |
|       * rho >= 2.0  -> 1 min Short Window (Burst Fault Storms)          |
|       * 1.3 <= rho < 2.0 -> 5 min Medium Window (Rising Activity)       |
|       * rho < 1.3   -> 10 min Long Window (Calm Baseline)               |
|   - Hysteresis: Cooldown of 2 calm bins before window expansion         |
|   - Minimum-log merging (< 5 logs merged into subsequent window)        |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                   STAGE 3: 14 STATISTICAL FEATURES EXTRACTION           |
|   total_logs, error_count, warning_count, error_ratio, warning_ratio,   |
|   unique_templates, dominant_template_freq, rare_template_ratio,        |
|   new_template_count, avg_time_gap, max_time_gap, burst_rate,          |
|   template_entropy, window_length_seconds                               |
|   RobustScaler fit on Train Set only                                    |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|            STAGE 4: PARTITIONED PARALLEL DETECTION & FUSION             |
|   - Data sliced across N = 4 Partitions                                 |
|   - Local Isolation Forest: S_local in [0, 1] via Percentiles (1%, 99%) |
|   - Global Frequency Score: S_global = mean(-log(df(t)/N_total)) in [0,1|
|   - Fused Score: S_final = 0.60 * S_local + 0.40 * S_global             |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                   STAGE 5: EXPLAINABILITY & TRIAGE ENGINE               |
|   - Severity Classification:                                            |
|       [0.00, 0.30) Normal  | [0.30, 0.50) Low                           |
|       [0.50, 0.70) Medium  | [0.70, 0.90) High | [0.90, 1.00] Critical  |
|   - Robust Z-Score: z = (val - median) / (IQR / 1.349)                  |
|   - Root Cause Diagnosis & Top-3 Contributing Features                  |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|               STAGE 6: COSMOS WEB DASHBOARD & SQLITE STORE              |
|   - Flask REST API (host='0.0.0.0', port=5000)                          |
|   - Deep Space Black & Cosmos Blue Futuristic Dashboard                 |
|   - Interactive Anomaly Waveform Timeline                               |
|   - SQLite Persistence & CSV Export                                     |
+-------------------------------------------------------------------------+
```

---

## 3. Technology Simplification (Reference vs Proposed)

| Aspect | Reference Paper (Wu et al., 2026) | LogPulse (Proposed Lightweight System) |
| :--- | :--- | :--- |
| **Parsing** | Distributed Drain on Spark | Lightweight Drain with stable JSON persistence |
| **Execution** | Dedicated Apache Spark Cluster | Python Multiprocessing (Partitioned Parallelism) |
| **Indexing** | RTree Spatial Indexing | Not needed (Isolation Forest tree partitioning) |
| **MST** | Borůvka Minimum Spanning Tree | Removed in favor of randomized split trees |
| **Clustering** | Improved HDBSCAN ($O(n^2)$) | Isolation Forest ($O(n \cdot \log n)$) |
| **Partition Fusion**| Gaussian RBF Kernel Function | Global Frequency Inverse Log Score Fusion |
| **Scoring** | GLOSH Outlier Scoring | Percentile Normalized Score $S \in [0, 1]$ |
| **Windowing** | Rigid Fixed Time Windows | Adaptive Rate-Based Dynamic Ladder (1m / 5m / 10m) |
| **Adaptation** | Identified as Future Work | Active Incremental Buffer with Poisoning Guard |
| **Interface** | CLI / Research Scripts | Cosmos Web Dashboard + REST API |

---

## 4. Quickstart Guide

### Prerequisites
- Python 3.10+
- Linux, macOS, or Windows

### Installation
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run test suite
PYTHONPATH=. pytest tests/ -v

# 3. Launch the self-bootstrapping Flask application
python3 app.py
```

The application automatically boots up, bootstraps SQLite tables, generates synthetic benchmark datasets if real logs are absent, fits the models, and serves the dashboard on `http://0.0.0.0:5000` (or through the dev server proxy on port 3000).

---

## 5. REST API Documentation

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/` | `GET` | Cosmos UI Web Dashboard |
| `/api/summary/<run_id>` | `GET` | Summary KPIs (total logs, anomalies, F1, precision, recall, FPR) |
| `/api/timeline/<run_id>` | `GET` | Anomaly timeline wave points with local and global scores |
| `/api/anomalies/<run_id>` | `GET` | Top detected anomalies with severity and primary reasons |
| `/anomaly/<id>` | `GET` | Drill-down diagnostic report (z-scores, raw logs, top features) |
| `/run` | `POST` | Trigger pipeline execution (`{"dataset": "...", "window_mode": "..."}`) |
| `/api/compare_windows/latest` | `GET` | Run ablation benchmark (Adaptive vs Fixed 1m, 5m, 10m) |
| `/download/<run_id>.csv` | `GET` | Download full window anomaly report in CSV format |
| `/upload` | `POST` | Ingest and evaluate user-uploaded log file |

---

## 6. Honest Limitations & Architectural Transparency

1. **Single-Node Parallelism**: Our parallelism utilizes Python multiprocessing over local CPU cores; it is not a multi-server distributed cluster.
2. **Feature Engineering**: Features are statistical and template-based. While lightweight and interpretable, they do not encode deep semantic embeddings (e.g., BERT/Transformers).
3. **Synthetic Ground Truth**: In offline environments without external internet access, metrics are evaluated against our deterministic synthetic dataset with injected fault patterns. Real dataset benchmarks must be cited separately.

---

## 7. Future Scope
- Real-time Kafka distributed streaming ingestion.
- Cloud native Kubernetes / Knative auto-scaling.
- Hybrid embedding representations using lightweight quantization (e.g., MiniLM).
- Automated alerting webhooks (Slack, PagerDuty).
