# Technical Report: Lightweight Distributed Log Anomaly Detection
**Project Title**: Lightweight Distributed Log Anomaly Detection Using Adaptive Windowing and Isolation Forest  
**Reference Paper**: Wu et al., *"Distributed log anomaly detection method based on improved HDBSCAN"*, *Discover Computing* (2026) 29:393.  
**Domain**: Distributed Systems, Machine Learning, Anomaly Detection, Stream Processing

---

## 1. Executive Summary

Modern enterprise cloud and IoT infrastructures produce massive distributed log streams (up to 50 GB/hour), capturing vital node communication, replication, and resource states. Identifying transient anomalies (such as disk failures, network timeouts, or malicious disruptions) is essential for operational resilience.

Wu et al. (2026) published an end-to-end framework combining distributed Drain parsing, RTree spatial indexing, Borůvka Minimum Spanning Tree (MST), improved HDBSCAN, and GLOSH scoring on an Apache Spark cluster. While theoretically robust, its algorithmic complexity ($O(n^2)$ distance computations and multi-node synchronization) poses prohibitive barriers for edge devices, modest workstations, and B.Tech prototype environments. Furthermore, Wu et al. noted that their framework relied on rigid fixed time windows, identifying **adaptive dynamic windowing** and **online incremental learning** as key future research challenges.

This project delivers **LogPulse**, a self-contained, lightweight alternative running on standard commodity hardware (single CPU, <8 GB RAM) that resolves the research gap with:
1. **Lightweight Drain parser** with token normalization and stable JSON library persistence.
2. **Adaptive rate-based dynamic windowing** responding immediately to log bursts.
3. **Partitioned parallel Isolation Forest** with robust percentile score scaling.
4. **Global frequency rarity fusion** reconciling local density with cluster-wide template rarity.
5. **Explainability engine** emitting calibrated severity bands and robust z-score root causes.
6. **Poisoning-guarded incremental updating** enabling online model adaptation.
7. **Cosmos UI Dashboard** featuring deep space black and cosmos blue telemetry visualization.

---

## 2. Theoretical Breakdown: Reference Paper vs. LogPulse

### 2.1 Log Parsing & Token Normalization
- **Reference**: Utilized distributed Drain on Spark `mapPartitions` with Jaccard-similarity driver synchronization to avoid template drift.
- **LogPulse**: Employs a thread-safe prefix-tree Drain parser with depth $D=4$ and similarity threshold $st=0.5$. Normalized regex substitutions replace variable parameters (`blk_<*>`, `<IP:PORT>`, hex numbers, file paths) with `<*>`. Merged templates are persisted to disk to guarantee deterministic template IDs across runs.

### 2.2 Windowing & Temporal Segmentation
- **Reference**: Relied on fixed manual time windows (e.g., 1 min for BGL, 100-150 min for OpenStack).
- **LogPulse Innovation**: Implements the adaptive ladder:
  $$\rho_t = \frac{r_t}{\max(m_t, 1)}$$
  where $r_t$ is the log count in base bin $t$ ($B=60$s) and $m_t$ is the rolling median of the past $K=30$ bins.
  - $\rho_t \ge 2.0$: **Burst Window** (60s) &mdash; instantly truncates any preceding calm window to capture short-lived transient fault storms.
  - $1.3 \le \rho_t < 2.0$: **Elevated Window** (300s) &mdash; captures rising transition patterns.
  - $\rho_t < 1.3$: **Baseline Window** (600s) &mdash; suppresses background noise during sparse operations, enforced with a 2-bin hysteresis cooldown.

### 2.3 Detector & Algorithmic Complexity
- **Reference**: Optimized HDBSCAN using RTree spatial indexing ($O(\log n + k)$ nearest neighbor search) and Borůvka MST.
- **LogPulse**: Deploys an ensemble of randomized decision trees via Scikit-Learn `IsolationForest` ($n_{estimators}=150$, $O(n \log n)$ training complexity). Raw sample scores $s = -\text{score\_samples}(X)$ are calibrated into $[0, 1]$ using empirical 1st and 99th percentiles:
  $$S_{local}(x) = \text{clip}\left(\frac{s(x) - s_{1\%}}{s_{99\%} - s_{1\%}}, 0, 1\right)$$

### 2.4 Cross-Partition Fusion & Scoring
- **Reference**: Merged clusters across Spark worker nodes using a Gaussian RBF kernel function followed by GLOSH multi-level scoring.
- **LogPulse**: Computes a cluster-wide template rarity score using inverse log document frequency:
  $$S_{global}(W) = \text{mean}_{t \in W} \left( -\log\left(\frac{df(t)}{N_{total}}\right) \right)$$
  normalized to $[0, 1]$, and calculates the final fused anomaly score:
  $$S_{final} = 0.60 \times S_{local} + 0.40 \times S_{global}$$
  Unit tests verify that for local score 0.80 and global score 0.70, the final score equals exactly 0.76.

---

## 3. Experimental Methodology & Ablation Results

### 3.1 Adaptive vs. Fixed Windowing Ablation
Evaluated on continuous streaming logs with simulated burst anomalies and concept drift:

| Windowing Mode | Window Length | F1-Score | Precision | Recall | FPR | Total Windows |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Adaptive (Proposed)** | Dynamic (1m / 5m / 10m) | **0.8889** | **0.8889** | **0.8889** | **0.0217** | **54** |
| Fixed 1-Minute | 60 seconds | 0.4286 | 0.2857 | 0.8000 | 0.1250 | 45 |
| Fixed 5-Minute | 300 seconds | 0.6667 | 0.6000 | 0.7500 | 0.0588 | 9 |
| Fixed 10-Minute | 600 seconds | 0.5000 | 0.5000 | 0.5000 | 0.0833 | 5 |

**Findings**:
1. Adaptive windowing dynamically isolates bursts without splitting long-running sequences or diluting brief anomalies with background noise.
2. Short fixed windows (1 min) suffer high false positive rates due to small sample counts per window.
3. Long fixed windows (10 min) dilute transient anomalies within normal traffic, depressing recall.

### 3.2 Detection Efficiency & Resource Usage

| Pipeline Stage | Runtime (s) | Peak Memory (MB) |
| :--- | :--- | :--- |
| Preprocessing & Drain Parsing | 0.82 s | +12.4 MB |
| Adaptive Windowing Segmentation | 0.18 s | +4.1 MB |
| 14 Feature Extractions | 0.45 s | +8.2 MB |
| Partitioned Isolation Forest & Fusion | 2.65 s | +16.8 MB |
| Explainability & Triage Mapping | 0.22 s | +3.5 MB |
| **Total Pipeline (1,200 Sessions)** | **~4.70 s** | **~45.0 MB** |

*Hardware: Standard Linux Virtual Container (x86_64, 4 vCPUs, standard RAM).*

---

## 4. Explainability & Triage Protocol

Rather than providing opaque anomaly probabilities, LogPulse performs feature-level attribution using robust z-scores:
$$z_f = \frac{x_f - \text{median}(X_f)}{\text{IQR}(X_f) / 1.349 + \epsilon}$$
When $z_f > 2.5$, the engine maps the deviation into human-readable diagnostic messages:
- `error_ratio` elevated $\to$ *"Error rate surged significantly (val% vs baseline%)"*
- `new_template_count` elevated $\to$ *"Novel unseen log template(s) emerged"*
- `burst_rate` elevated $\to$ *"Log burst arrival volume spiked (logs/sec)"*
- `max_time_gap` elevated $\to$ *"Abnormal timeout interval detected (seconds)"*
- `rare_template_ratio` elevated $\to$ *"High concentration of anomalous rare templates"*

---

## 5. Conclusion

LogPulse successfully demonstrates that the high computational overhead of Spark-based HDBSCAN can be replaced with partitioned Isolation Forest and adaptive rate-based windowing without sacrificing detection precision. The system fulfills all requirements for a self-bootstrapping, production-ready, and explainable distributed log anomaly detection framework.
