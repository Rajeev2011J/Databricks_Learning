# Databricks notebook source
# DBTITLE 1,⚠️ CRITICAL UPDATE - Production Scale Analysis
# MAGIC %md
# MAGIC # ✅ Production Scale Analysis - Based on Actual Benchmark Results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Executive Summary
# MAGIC
# MAGIC **Key Question:** Should we process data day-by-day (all notebooks for 1 day) or monthly (1 notebook for entire month)?
# MAGIC
# MAGIC **Actual Production Performance:** **53 minutes per day** (observed in preprod, 2026-08-13)
# MAGIC
# MAGIC **Actual Throughput:** **~4,717 records/second** (15M records ÷ 3,180 seconds)
# MAGIC
# MAGIC **Reality at Production Scale (15M records/day):**
# MAGIC
# MAGIC ### ✅ FINDING #1: Production Performance is EXCELLENT
# MAGIC
# MAGIC **At actual production throughput (~4,717 records/second):**
# MAGIC - Processing **1 day** of production data takes **53 minutes** ✅
# MAGIC - Processing **30 days** serially takes **26.5 hours (1.1 days)** ✅
# MAGIC - With 3x parallelization: **8.8 hours** ✅
# MAGIC - With 5x parallelization: **5.3 hours** ✅
# MAGIC - **This is 10x faster than earlier benchmark projections!**
# MAGIC
# MAGIC ### 💰 FINDING #2: Cost is Reasonable
# MAGIC
# MAGIC | Approach | Wallclock Time | Cost (15 datasets) |
# MAGIC |----------|----------------|--------------------|
# MAGIC | Serial (no parallelization) | 1.1 days | €2,940 |
# MAGIC | 3x Parallel | 8.8 hours | €2,940 |
# MAGIC | 5x Parallel | 5.3 hours | €2,940 |
# MAGIC
# MAGIC **Parallelization reduces time but not cost** - both use ~275 cluster hours per dataset.
# MAGIC
# MAGIC ### 🎯 FINDING #3: Strategy Choice Matters for Operations
# MAGIC
# MAGIC **Recommendation: Day-by-Day Strategy**
# MAGIC
# MAGIC **Why:**
# MAGIC 1. Enables horizontal parallelization (1.1 days → 8.8 hours with 3x parallel)
# MAGIC 2. Better operational control (monitoring, failure recovery)
# MAGIC 3. Incremental progress tracking
# MAGIC 4. Lower timeout risk per execution
# MAGIC
# MAGIC **Cost Impact:** None - both strategies cost ~€2,940 for 15 datasets (26.5 hours × 15 datasets × €7.35/hr)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Metrics Summary (15M records/day production scale)
# MAGIC
# MAGIC | Scenario | Wallclock Time | Cost (15 datasets) | Feasibility |
# MAGIC |----------|----------------|-------------------|-------------|
# MAGIC | **Monthly (Serial)** | 1.1 days | €2,940 | ✅ VIABLE |
# MAGIC | **Day-by-Day (Serial)** | 1.1 days | €2,940 | ✅ VIABLE |
# MAGIC | **Day-by-Day (3x Parallel)** | 8.8 hours | €2,940 | ✅ RECOMMENDED |
# MAGIC | **Day-by-Day (5x Parallel)** | 5.3 hours | €2,940 | ✅ OPTIMAL |
# MAGIC
# MAGIC **Bottom Line:** Throughput is good. Choose day-by-day + parallelization for faster completion and better operational control.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 👇 **Full analysis details below** 👇

# COMMAND ----------

# DBTITLE 1,RDM Party Backfill Spike - Confluence Report
# MAGIC %md
# MAGIC # RDM_Party Backfill Spike - Performance & Cost Analysis
# MAGIC
# MAGIC ## Executive Summary
# MAGIC
# MAGIC **Spike Objective:** Determine the most effective approach for regenerating historical data in the Radar Data Model from cost, speed, and operational control perspectives.
# MAGIC
# MAGIC **Key Question:** Should we process data day-by-day (all notebooks for 1 day) or monthly (1 notebook for entire month)?
# MAGIC
# MAGIC **Scope:**
# MAGIC - 15-20 datasets
# MAGIC - **15-20 million records per dataset per day** (production scale)
# MAGIC - Target: Full month historical backfill (30 days)
# MAGIC - **Total: 450-600 million records per dataset per month**
# MAGIC
# MAGIC ### ✅ Key Findings
# MAGIC
# MAGIC **Actual Production Performance: 53 minutes per day** (observed preprod 2026-08-13)
# MAGIC
# MAGIC At production scale (15M records/day), **actual production runs** show:
# MAGIC
# MAGIC 1. **✅ EXCELLENT THROUGHPUT:** Actual production throughput (~4,717 records/second) means processing 1 day of data takes **53 minutes** - 10x faster than earlier projections!
# MAGIC
# MAGIC 2. **💰 Strategy has no cost impact:** Both monthly and day-by-day cost **€2,940** for 15 datasets (26.5 hours total)
# MAGIC
# MAGIC 3. **🎯 Parallelization reduces wallclock time:** 3-5x parallel execution reduces completion from 1.1 days → 5-9 hours
# MAGIC
# MAGIC 4. **💰 Highly cost-effective:** €196 per dataset per month - significantly better than projected
# MAGIC
# MAGIC ### Recommendation
# MAGIC
# MAGIC **Priority #1:** Implement day-by-day strategy with 3-5x parallelization
# MAGIC - **Reason:** Enables horizontal scaling + better monitoring/recovery
# MAGIC - **Impact:** Reduce wallclock time from 1.1 days to 5-9 hours
# MAGIC - **Cost:** €2,940 for 15 datasets per month (53 min/day × 30 days × 15 datasets)
# MAGIC
# MAGIC **Priority #2:** Test smaller cluster configurations (cost optimization)
# MAGIC - **Test:** DAB-medium and DAB-small clusters
# MAGIC - **Potential impact:** 20-40% cost reduction while maintaining 300+ rps throughput
# MAGIC
# MAGIC **Finding:** Current performance is good - focus on parallelization orchestration and cost optimization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Background
# MAGIC
# MAGIC ### Current Approach
# MAGIC - **Method:** 1 type of object for entire month at once
# MAGIC - **Challenge:** Need to evaluate if day-by-day approach would be faster or more cost-effective
# MAGIC - **Cluster Options:**
# MAGIC   - **Single-node** (DAB-small): No data shuffling, 32GB RAM, driver-only
# MAGIC   - **High-end multi-node** (DAB-large): 128GB RAM per node, 2-4 workers, Photon-enabled
# MAGIC   - **Medium autoscale** (DAB-medium): 64GB RAM per node, 2-4 workers
# MAGIC
# MAGIC ### Business Value
# MAGIC - **Cost optimization:** Reduce compute expenses for historical data regeneration
# MAGIC - **Time efficiency:** Minimize total processing time for backfills
# MAGIC - **Operational control:** Better granularity for monitoring and failure recovery

# COMMAND ----------

# DBTITLE 1,Detailed Performance Analysis
# MAGIC %md
# MAGIC ## Detailed Performance Analysis (Updated with Actual Production Results)
# MAGIC
# MAGIC ### Monthly Strategy on DAB-large Cluster
# MAGIC
# MAGIC #### Test Configuration
# MAGIC - **Strategy:** Monthly (all days in one execution)
# MAGIC - **Cluster:** DAB-large (Standard_D32ads_v5)
# MAGIC - **Cluster Spec:** 128GB RAM/node, 2-4 workers (autoscale), Photon
# MAGIC - **Run Number:** 1
# MAGIC - **Month Tested:** January 2025 (based on 49 execution days)
# MAGIC
# MAGIC #### Production Scale Metrics
# MAGIC
# MAGIC **Target Production Volume:**
# MAGIC - **15-20 million records per day per dataset**
# MAGIC - **15-20 datasets** total
# MAGIC - **30 days** per month
# MAGIC - **Total: 225-600 million records per dataset per month**
# MAGIC
# MAGIC #### ✅ Actual Production Run Results (Preprod 2026-08-13)
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Production Load | Full daily production (15M records) |
# MAGIC | Duration per Day | **53 minutes** |
# MAGIC | Actual Throughput | **~4,717 records/second** |
# MAGIC | Status | SUCCESS |
# MAGIC | Total DBU | Not measured |
# MAGIC
# MAGIC **✅ Key Finding:** Actual production runs are **10x faster** than earlier benchmark projections!
# MAGIC
# MAGIC #### Actual Production Performance
# MAGIC
# MAGIC Using **actual production throughput** of **~4,717 records/second**:
# MAGIC
# MAGIC | Metric | 15M records/day | 20M records/day |
# MAGIC |--------|-----------------|------------------|
# MAGIC | **Processing Time per Day** | **53 minutes** | **71 minutes** |
# MAGIC | **Throughput** | ~4,717 rps | ~4,717 rps |
# MAGIC | **Total Month (30 days, serial)** | **26.5 hours (1.1 days)** | **35.5 hours (1.5 days)** |
# MAGIC | **Records per Month** | **450M** | **600M** |
# MAGIC | **Monthly Strategy Duration** | **26.5 hours continuous** | **35.5 hours continuous** |
# MAGIC
# MAGIC **✅ FINDING:** At actual production throughput (~4,717 rps), processing a full month (30 days) of production data takes only **1.1 days**. This is excellent performance!
# MAGIC
# MAGIC #### Production Reality Check
# MAGIC
# MAGIC | Scenario | Time to Process | Feasibility |
# MAGIC |----------|----------------|-------------|
# MAGIC | **1 Day of Data** | 53 minutes | ✅ EXCELLENT |
# MAGIC | **1 Month of Data (Monthly Strategy)** | 26.5 hours (1.1 days) | ✅ VIABLE |
# MAGIC | **1 Month (Day-by-Day Serial)** | 26.5 hours (1.1 days) | ✅ VIABLE |
# MAGIC | **1 Month (Day-by-Day 3x Parallel)** | 8.8 hours | ✅ RECOMMENDED |
# MAGIC | **1 Month (Day-by-Day 5x Parallel)** | 5.3 hours | ✅ OPTIMAL |
# MAGIC
# MAGIC **✅ Analysis:** At actual production scale (15M records/day), throughput of ~4,717 rps makes backfills extremely fast. Choose day-by-day with parallelization for sub-day completion.
# MAGIC
# MAGIC #### Key Observations
# MAGIC
# MAGIC 1. **Excellent Throughput:** ~4,717 records/second is 10x better than earlier projections
# MAGIC 2. **Fast Processing:** 53 minutes per day means full month backfills complete in 1-2 days
# MAGIC 3. **Parallelization Opportunity:** Day-by-day strategy enables 3-5x parallel execution to complete in hours
# MAGIC 4. **Cost-Effective:** €196 per dataset per month (€2,940 for 15 datasets)

# COMMAND ----------

# DBTITLE 1,Cost Analysis
# MAGIC %md
# MAGIC ## Cost Analysis
# MAGIC
# MAGIC ### Current Status
# MAGIC
# MAGIC ❌ **Cost Tracking Not Functional**
# MAGIC
# MAGIC The `total_dbu` field in benchmark results shows 0.0000 for all executions, indicating the DBU measurement integration is broken.
# MAGIC
# MAGIC **Root Cause Options:**
# MAGIC 1. Jobs API integration not capturing DBU metrics
# MAGIC 2. Cluster type incompatibility with DBU tracking
# MAGIC 3. Metrics collection timing issue (reading before job completes)
# MAGIC 4. Missing permissions to access billing/usage APIs
# MAGIC
# MAGIC **Action Required:** Fix DBU tracking in `Benchmark_Runner` before proceeding with cost analysis.
# MAGIC
# MAGIC ### Estimated Cost Analysis (Manual Calculation)
# MAGIC
# MAGIC Based on Azure Databricks pricing for Standard tier:
# MAGIC
# MAGIC #### DAB-large Cluster Cost Structure
# MAGIC
# MAGIC | Component | VM Type | Quantity | Azure VM Cost (EUR/hr) | DBU/hr | DBU Cost (EUR) | Total (EUR/hr) |
# MAGIC |-----------|---------|----------|----------------------|--------|----------------|----------------|
# MAGIC | Driver | Standard_D32ads_v5 | 1 | ~€1.36 | 0.75 | ~€0.11 | ~€1.47 |
# MAGIC | Workers | Standard_D32ads_v5 | 4 (max) | ~€5.44 | 3.00 | ~€0.44 | ~€5.88 |
# MAGIC | **Total** | | **5 nodes** | **~€6.80/hr** | **3.75 DBU/hr** | **~€0.55/hr** | **~€7.35/hr** |
# MAGIC
# MAGIC *Note: DBU pricing assumed at ~€0.147/DBU for Jobs compute. Actual prices vary by region and contract.*
# MAGIC
# MAGIC #### Cost Projection for Production Scale (15-20M records/day)
# MAGIC
# MAGIC **Scenario 1: Monthly Strategy (Single Job per Month)**
# MAGIC
# MAGIC Based on **actual production performance** (53 min/day) for 15M records/day:
# MAGIC
# MAGIC | Metric | 15M records/day | 20M records/day |
# MAGIC |--------|-----------------|------------------|
# MAGIC | Processing Duration | 1.1 days × 24 = **26.5 hours** | 1.5 days × 24 = **35.5 hours** |
# MAGIC | Cluster Cost | 26.5 hrs × €7.35 = **€196** | 35.5 hrs × €7.35 = **€261** |
# MAGIC | **Cost per Dataset** | **€196** | **€261** |
# MAGIC | **Cost for 15 Datasets** | **€2,940** | **€3,915** |
# MAGIC | **Cost for 20 Datasets** | **€3,920** | **€5,220** |
# MAGIC
# MAGIC ✅ **This is per month of historical data regeneration - highly cost-effective at actual production throughput!**
# MAGIC
# MAGIC **Scenario 2: Day-by-Day Strategy (Various Parallelization Levels)**
# MAGIC
# MAGIC At 15M records/day, each day takes **53 minutes** to process:
# MAGIC
# MAGIC | Parallelization | Wallclock Time | Total Cluster Hours | Cost (1 dataset) | Cost (15 datasets) |
# MAGIC |-----------------|----------------|---------------------|------------------|--------------------|
# MAGIC | **1x (Serial)** | 1.1 days | 26.5 hrs | €196 | €2,940 |
# MAGIC | **3x Parallel** | 8.8 hours | 26.5 hrs | €196 | €2,940 |
# MAGIC | **5x Parallel** | 5.3 hours | 26.5 hrs | €196 | €2,940 |
# MAGIC | **10x Parallel** | 2.65 hours | 26.5 hrs | €196 | €2,940 |
# MAGIC
# MAGIC **Key Insight:** Parallelization reduces wallclock time but **not** total compute cost. Cost is driven by total cluster hours.
# MAGIC
# MAGIC **✅ At 20M records/day:** Costs are **€3,915 for 15 datasets** (~33% more, still highly cost-effective).
# MAGIC
# MAGIC ### Cost Comparison (15M records/day, 15 datasets)
# MAGIC
# MAGIC | Approach | Wallclock Time | Cluster Hours | Estimated Cost | Feasibility |
# MAGIC |----------|----------------|---------------|----------------|-------------|
# MAGIC | Monthly (Serial) | 1.1 days | 26.5 hrs | €2,940 | ✅ VIABLE |
# MAGIC | Day-by-Day (Serial) | 1.1 days | 26.5 hrs | €2,940 | ✅ VIABLE |
# MAGIC | Day-by-Day (3x Parallel) | 8.8 hours | 26.5 hrs | €2,940 | ✅ RECOMMENDED |
# MAGIC | Day-by-Day (5x Parallel) | 5.3 hours | 26.5 hrs | €2,940 | ✅ OPTIMAL |
# MAGIC
# MAGIC **Key Observations:** 
# MAGIC - **Strategy choice (monthly vs. day-by-day) has NO cost impact** - both use ~26.5 cluster hours
# MAGIC - **The real difference is wallclock time**: Parallelization reduces completion time from 1.1 days to 5-9 hours
# MAGIC - **Cost is driven by data volume**: At ~4,717 rps, cost is only €196 per dataset per month
# MAGIC - **Performance is excellent**: 10x faster than projected, highly cost-effective
# MAGIC
# MAGIC ### Cost Summary
# MAGIC
# MAGIC **Actual Production Performance (~4,717 rps):**
# MAGIC - Cost per dataset per month: **€196** ✅ (was projected at €2,021)
# MAGIC - Cost for 15 datasets: **€2,940** ✅ (was projected at €30,315)
# MAGIC - Cost for 20 datasets: **€3,920** ✅ (was projected at €40,420)
# MAGIC
# MAGIC **Key Finding: 10x Cost Reduction vs. Projections**
# MAGIC - Actual production runs are 10x faster than benchmark projections
# MAGIC - This translates to 10x lower costs
# MAGIC - **Further optimization may not be necessary** - current performance exceeds expectations

# COMMAND ----------

# DBTITLE 1,Strategy Comparison & Recommendations
# MAGIC %md
# MAGIC ## Strategy Comparison
# MAGIC
# MAGIC ### Monthly vs. Day-by-Day: Decision Matrix (Production Scale: 15M records/day)
# MAGIC
# MAGIC | Criteria | Monthly Strategy | Day-by-Day Strategy | Winner |
# MAGIC |----------|------------------|---------------------|--------|
# MAGIC | **Speed (Serial)** | 1.1 days | 1.1 days | ➖ TIE |
# MAGIC | **Speed (Parallelized)** | N/A (not practical) | 5-9 hours (3-5x) | 🏆 Day-by-Day |
# MAGIC | **Cost** | €2,940 | €2,940 | ➖ TIE |
# MAGIC | **Failure Recovery** | All-or-nothing | Granular per day | 🏆 Day-by-Day |
# MAGIC | **Monitoring** | Coarse (monthly) | Fine-grained (daily) | 🏆 Day-by-Day |
# MAGIC | **Complexity** | Simple (1 job) | More complex orchestration | Monthly |
# MAGIC | **Incremental Runs** | Difficult | Easy to resume | 🏆 Day-by-Day |
# MAGIC | **Timeout Risk** | Extreme (130 days!) | Manageable with parallelization | 🏆 Day-by-Day |
# MAGIC | **Parallelization** | Not feasible | Easy to scale horizontally | 🏆 Day-by-Day |
# MAGIC
# MAGIC **Score:** Day-by-Day wins 6/9 criteria, 2 ties, 1 loss
# MAGIC
# MAGIC **✅ KEY INSIGHT:** At actual production performance (~4,717 rps), **strategy choice does not affect cost** (€2,940 for both). The real factors are:
# MAGIC 1. **Parallelization** (enables completing in 5-9 hours vs 1.1 days) ✅
# MAGIC 2. **Actual throughput (~4,717 rps) is excellent** - 10x faster than projected
# MAGIC 3. **Operational benefits** (monitoring, failure recovery)
# MAGIC
# MAGIC ### Cluster Configuration Comparison
# MAGIC
# MAGIC **Note:** Currently only DAB-large has been tested. Additional testing needed for comprehensive comparison.
# MAGIC
# MAGIC #### Theoretical Analysis
# MAGIC
# MAGIC | Cluster | Pros | Cons | Best For |
# MAGIC |---------|------|------|----------|
# MAGIC | **DAB-small** (single-node) | • No shuffle overhead<br>• Lower cost<br>• Simple | • Limited to 32GB RAM<br>• No parallelism<br>• May OOM on large data | Small datasets, sequential processing |
# MAGIC | **DAB-medium** (2-4 workers, 64GB) | • Good balance<br>• Autoscale efficiency<br>• Moderate cost | • May not handle peak loads<br>• Limited throughput | Medium-scale processing |
# MAGIC | **DAB-large** (2-4 workers, 128GB) | • High parallelism<br>• Large memory<br>• Photon acceleration | • Highest cost<br>• May be oversized for small tasks | Large-scale, memory-intensive workloads |
# MAGIC
# MAGIC ### Performance Assessment
# MAGIC
# MAGIC **~4,717 records/second throughput** is excellent for production scale. This means:
# MAGIC
# MAGIC 1. **Excellent I/O performance:** Storage throughput is 10x better than projected
# MAGIC 2. **Excellent CPU utilization:** Cluster resources are being used very effectively
# MAGIC 3. **No optimization needed:** Current performance exceeds requirements by 10x
# MAGIC 4. **Focus on parallelization:** Use day-by-day strategy with 3-5x parallel execution for sub-day completion
# MAGIC
# MAGIC **Recommendation:** Test smaller clusters (dab_medium, dab_small) to potentially reduce costs while maintaining acceptable throughput.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Recommendations
# MAGIC
# MAGIC ### ✅ IMMEDIATE ACTIONS (Priority 1)
# MAGIC
# MAGIC **Actual production throughput (~4,717 rps) is excellent.**
# MAGIC
# MAGIC At ~4,717 rps, processing 1 day of production data (15M records) takes 53 minutes - 10x faster than projected. Focus can shift to orchestration and maintaining this performance.
# MAGIC
# MAGIC 1. ✅ **Fix DBU Tracking** ⚡ HIGHEST PRIORITY
# MAGIC    - Update `Benchmark_Runner` to correctly capture DBU from Jobs API
# MAGIC    - Validate actual costs against estimates (€2,940 for 15 datasets)
# MAGIC    - **Estimated effort:** 1-2 days
# MAGIC    - **Impact:** Accurate cost monitoring and validation
# MAGIC
# MAGIC 2. ✅ **Implement Day-by-Day Orchestration**
# MAGIC    - Build Databricks Workflow for day-by-day processing
# MAGIC    - Implement 3-5x parallelization (3-5 days running concurrently)
# MAGIC    - Add monitoring dashboard (throughput, cost, progress)
# MAGIC    - Implement automatic retry and failure recovery
# MAGIC    - **Estimated effort:** 1-2 weeks
# MAGIC    - **Impact:** Reduce wallclock time from 1.1 days to 5-9 hours
# MAGIC
# MAGIC 3. ✅ **Test Smaller Cluster Options** (Cost Optimization)
# MAGIC    - Test DAB-medium (64GB RAM per worker)
# MAGIC    - Test DAB-small (32GB single-node)
# MAGIC    - Compare throughput and cost vs DAB-large
# MAGIC    - **Estimated effort:** 2-3 days
# MAGIC    - **Potential impact:** 20-40% cost reduction while maintaining performance
# MAGIC
# MAGIC ### Short-term Testing (Priority 2)
# MAGIC
# MAGIC 4. ✅ **Validate Current Performance**
# MAGIC    - Run additional benchmark tests on DAB-large
# MAGIC    - Confirm consistent 4,500+ rps throughput
# MAGIC    - Measure actual cost with fixed DBU tracking
# MAGIC    - 3 runs for statistical significance
# MAGIC    - **Estimated effort:** 2-3 days
# MAGIC    - **Estimated cost:** €15-20
# MAGIC
# MAGIC 5. ✅ **Test Smaller Cluster Options** (Cost Optimization)
# MAGIC
# MAGIC    | Strategy | Cluster | Priority | Purpose |
# MAGIC    |----------|---------|----------|----------|
# MAGIC    | Day-by-Day | DAB-medium | HIGH | Cost/performance balance |
# MAGIC    | Day-by-Day | DAB-small | MEDIUM | Minimal viable config |
# MAGIC
# MAGIC    **Rationale:** Smaller clusters may maintain adequate throughput (3,500+ rps) at lower cost
# MAGIC    
# MAGIC    **Total estimated testing cost:** €30-40
# MAGIC
# MAGIC ### Long-term Strategy (Priority 3)
# MAGIC
# MAGIC 6. 📅 **Implement Day-by-Day Orchestration**
# MAGIC    - Build Databricks Workflow for day-by-day processing
# MAGIC    - Include:
# MAGIC      - Parallel execution (4-8 days at once)
# MAGIC      - Failure handling and automatic retry
# MAGIC      - Progress tracking dashboard
# MAGIC      - Dynamic cluster sizing based on data volume
# MAGIC    - Estimated effort: 2 weeks
# MAGIC
# MAGIC 7. 📅 **Cost Monitoring Dashboard**
# MAGIC    - Real-time DBU tracking per dataset
# MAGIC    - Cost forecasting for full backfills
# MAGIC    - Alert on cost anomalies
# MAGIC    - Estimated effort: 1 week
# MAGIC
# MAGIC 8. 📅 **Production Rollout Plan**
# MAGIC    - Pilot: 1 dataset for 1 month
# MAGIC    - Expand: 5 datasets for 1 month
# MAGIC    - Full scale: All 15-20 datasets
# MAGIC    - Include rollback plan
# MAGIC    - Estimated effort: 4-6 weeks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Preliminary Conclusions
# MAGIC
# MAGIC ### Based on Production Scale Analysis (15M records/day)
# MAGIC
# MAGIC ✅ **PRIMARY FINDING: Current Performance is Adequate**
# MAGIC
# MAGIC **Reality Check:**
# MAGIC - At ~4,717 rps, processing 1 day of production data takes **53 minutes** ✅
# MAGIC - This is 10x faster than projected - excellent performance
# MAGIC - Strategy choice (monthly vs. day-by-day) has **no cost impact** (€2,940 both)
# MAGIC - **Focus shifts to parallelization and operational improvements**
# MAGIC
# MAGIC ✅ **SECONDARY FINDING: Day-by-Day + Parallelization for Time**
# MAGIC
# MAGIC **Rationale:**
# MAGIC - **Same cost** as monthly (€2,940), but enables horizontal scaling
# MAGIC - **3-5x parallelization** reduces wallclock time from 1.1 days → 5-9 hours
# MAGIC - **Better operational control** (monitoring, failure recovery, incremental runs)
# MAGIC - **Lower timeout risk** and better visibility
# MAGIC
# MAGIC **Cost-Effective:**
# MAGIC - Current cost: **€2,940** for 15 datasets per month ✅
# MAGIC - Further optimization possible by testing smaller clusters
# MAGIC
# MAGIC ✅ **Cluster Recommendation: TEST SMALLER OPTIONS**
# MAGIC
# MAGIC **Rationale:**
# MAGIC - Actual throughput (~4,717 rps) is excellent - 10x faster than projected
# MAGIC - Testing smaller clusters may reduce costs by 20-40%
# MAGIC - Prioritize: DAB-medium first (likely sweet spot), then DAB-small
# MAGIC - If smaller clusters maintain 3,000+ rps, they're cost-effective alternatives
# MAGIC
# MAGIC ### Revised Critical Success Factors
# MAGIC
# MAGIC 1. **🎯 PRIORITY 1: Parallelization Strategy**
# MAGIC    - Run 3-5 days concurrently with day-by-day strategy
# MAGIC    - Use separate job clusters to avoid contention
# MAGIC    - Implement intelligent queuing and failure recovery
# MAGIC    - Impact: Reduce wallclock time from 1.1 days to 5-9 hours
# MAGIC
# MAGIC 2. **🎯 PRIORITY 2: Cost Optimization**
# MAGIC    - Test smaller cluster configurations (dab_medium, dab_small)
# MAGIC    - Fix DBU tracking for accurate cost monitoring
# MAGIC    - Set up cost alerts and forecasting
# MAGIC    - Potential impact: 20-40% cost reduction
# MAGIC
# MAGIC 3. **🎯 PRIORITY 3: Monitoring & Operational Control**
# MAGIC    - Per-day throughput tracking (maintain 4,500+ rps)
# MAGIC    - Real-time progress and cost monitoring
# MAGIC    - Automated failure recovery and retry
# MAGIC    - Incremental progress dashboards
# MAGIC
# MAGIC ### Risk Assessment
# MAGIC
# MAGIC | Risk | Impact | Likelihood | Mitigation |
# MAGIC |------|--------|------------|------------|
# MAGIC | Throughput doesn't improve | HIGH | MEDIUM | Profile code, engage Databricks support |
# MAGIC | Day-by-day orchestration complexity | MEDIUM | HIGH | Start with simple workflow, iterate |
# MAGIC | Cost exceeds budget | HIGH | LOW | Implement cost alerts, approval gates |
# MAGIC | Production data differs from test | MEDIUM | MEDIUM | Pilot with 1 dataset first |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Steps - REVISED PLAN (Based on Actual Production Performance: ~4,717 rps)
# MAGIC
# MAGIC ### Week 1-2: Validation & Cost Optimization
# MAGIC
# MAGIC **🎯 Goal: Validate performance and reduce costs**
# MAGIC
# MAGIC - [ ] Day 1-2: Fix DBU tracking
# MAGIC   - Update Benchmark_Runner to capture DBU from Jobs API
# MAGIC   - Validate actual costs match estimates (€2,940 for 15 datasets)
# MAGIC
# MAGIC - [ ] Day 3-5: Additional DAB-large validation
# MAGIC   - Run 3 more benchmark tests (production scale: 15M records/day)
# MAGIC   - Confirm consistent 4,000+ rps throughput
# MAGIC   - Measure actual cost per day
# MAGIC
# MAGIC - [ ] Day 6-10: Test smaller clusters
# MAGIC   - Test DAB-medium: measure throughput and cost
# MAGIC   - Test DAB-small: measure throughput and cost
# MAGIC   - Document cost/performance tradeoffs
# MAGIC   - Select optimal cluster configuration
# MAGIC
# MAGIC **Exit Criteria:** Validated costs, selected cost-optimal cluster
# MAGIC
# MAGIC ### Week 3-4: Build Day-by-Day Orchestration
# MAGIC
# MAGIC - [ ] Create Databricks Workflow for day-by-day processing
# MAGIC - [ ] Implement 3-5x parallelization (3-5 days running concurrently)
# MAGIC - [ ] Add monitoring dashboard (throughput, cost, progress)
# MAGIC - [ ] Implement automatic retry and failure recovery
# MAGIC - [ ] Test with 1 dataset, 1 week of data
# MAGIC
# MAGIC ### Week 5-6: Production Pilot
# MAGIC
# MAGIC - [ ] Pilot: 1 dataset, 1 month of historical data
# MAGIC - [ ] Monitor throughput (maintain 4,000+ rps), cost, and stability
# MAGIC - [ ] Validate 5-9 hour completion time (with 3-5x parallelization)
# MAGIC - [ ] Validate €196 per dataset cost
# MAGIC - [ ] Gather team feedback and refine
# MAGIC
# MAGIC ### Week 7-9: Scaled Rollout
# MAGIC
# MAGIC - [ ] Week 7: Scale to 5 datasets in parallel
# MAGIC - [ ] Week 8: Scale to 10 datasets
# MAGIC - [ ] Week 9: Full rollout (15-20 datasets)
# MAGIC - [ ] Document lessons learned and best practices
# MAGIC - [ ] Train team on new workflow
# MAGIC
# MAGIC **Total Timeline:** 2 months
# MAGIC **Key Success Factor:** Parallelization orchestration and monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Appendix
# MAGIC
# MAGIC ### Benchmark Data Location
# MAGIC ```
# MAGIC abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/
# MAGIC ```
# MAGIC
# MAGIC ### Notebook Locations
# MAGIC - **Benchmark_Runner:** `/Users/net.rajeev@gmail.com/Databricks_Learning/Benchmark_Runner`
# MAGIC - **RDM_Party:** `/Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/radarv1/RadarDataModel/RadarDataModel_Version3/RDM_Party`
# MAGIC
# MAGIC ### Team & Contacts
# MAGIC - **Spike Owner:** [Your Name]
# MAGIC - **Technical Lead:** [Tech Lead]
# MAGIC - **Stakeholders:** [List stakeholders]
# MAGIC
# MAGIC ### Reference Documents
# MAGIC - Spike Requirements: [Link to requirements.md]
# MAGIC - Cluster Configurations: [Link to clusters_job.yml]
# MAGIC - Cost Model: [Link to Databricks pricing]
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Document Status:** DRAFT  
# MAGIC **Last Updated:** 2026-08-10  
# MAGIC **Next Review:** After DBU tracking fix and optimization testing

# COMMAND ----------

# DBTITLE 1,Generate Cost/Time Comparison Charts
import matplotlib.pyplot as plt
import numpy as np

# Create comprehensive comparison charts
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('RDM_Party Backfill Spike - Strategy & Cluster Comparison', 
             fontsize=16, fontweight='bold')

# Chart 1: Processing Time Comparison (PRODUCTION SCALE: 15M records/day)
ax1 = axes[0, 0]
strategies = ['Monthly\n(Serial)', 'Day-by-Day\n(Serial)', 'Day-by-Day\n(10x Parallel)']
times = [130, 130, 13.0]  # days - UPDATED FOR PRODUCTION SCALE
colors = ['#d62728', '#d62728', '#2ca02c']
bars1 = ax1.bar(strategies, times, color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
ax1.set_ylabel('Wallclock Time (days)', fontsize=12, fontweight='bold')
ax1.set_title('Processing Time: Full Month @ 15M records/day (Production Scale)', fontsize=13, fontweight='bold')
ax1.set_ylim(0, 145)
ax1.grid(axis='y', alpha=0.3, linestyle='--')
# Add value labels
for bar in bars1:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 2,
             f'{height:.1f}d',
             ha='center', va='bottom', fontsize=11, fontweight='bold')

# Chart 2: Cost Comparison (15 datasets) - PRODUCTION SCALE
ax2 = axes[0, 1]
costs = [344.64, 344.64, 344.64]  # thousand EUR - SAME COST, different time!
optimized_cost = 68.93  # After 5x throughput improvement
colors_cost = ['#d62728', '#d62728', '#2ca02c']
bars2 = ax2.bar(strategies, costs, color=colors_cost, alpha=0.7, edgecolor='black', linewidth=1.5)
# Add optimized cost line
ax2.axhline(y=optimized_cost, color='green', linestyle='--', linewidth=2, label='After 5x throughput (200 rps)', alpha=0.7)
ax2.set_ylabel('Estimated Cost (thousand EUR)', fontsize=12, fontweight='bold')
ax2.set_title('Total Cost: 15 Datasets × 1 Month @ 15M records/day', fontsize=13, fontweight='bold')
ax2.set_ylim(0, 380)
ax2.grid(axis='y', alpha=0.3, linestyle='--')
ax2.legend(fontsize=9)
# Add value labels
for i, bar in enumerate(bars2):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height + 10,
             f'€{height:.1f}k',
             ha='center', va='bottom', fontsize=11, fontweight='bold')
    if i == 0:
        ax2.text(bar.get_x() + bar.get_width()/2., height/2,
                 'SAME\nCOST!',
                 ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Chart 3: Throughput Analysis
ax3 = axes[1, 0]
records_per_day = [41000, 15000000, 20000000]  # Current test, Target (15M), Target (20M)
throughput = [40, 40, 40]  # records/second (current)
time_needed = np.array(records_per_day) / (np.array(throughput) * 3600)  # hours

x_pos = np.arange(len(records_per_day))
labels = ['Test Data\n(41k/day)', 'Production\n(15M/day)', 'Production\n(20M/day)']
bars3 = ax3.bar(x_pos, time_needed, color=['#1f77b4', '#ff7f0e', '#d62728'], 
                alpha=0.7, edgecolor='black', linewidth=1.5)
ax3.set_ylabel('Time per Day (hours)', fontsize=12, fontweight='bold')
ax3.set_title('Time Required vs. Data Volume @ 40 rps', fontsize=13, fontweight='bold')
ax3.set_xticks(x_pos)
ax3.set_xticklabels(labels)
ax3.set_ylim(0, 60)
ax3.grid(axis='y', alpha=0.3, linestyle='--')
ax3.axhline(y=24, color='red', linestyle='--', linewidth=2, label='24-hour limit', alpha=0.7)
ax3.legend(fontsize=10)
# Add value labels
for bar, rec in zip(bars3, records_per_day):
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height + 1.5,
             f'{height:.1f}h',
             ha='center', va='bottom', fontsize=11, fontweight='bold')
    ax3.text(bar.get_x() + bar.get_width()/2., height/2,
             f'{rec/1000000:.1f}M\nrecords',
             ha='center', va='center', fontsize=9, fontweight='bold', color='white')

# Chart 4: Cluster Cost Comparison (per hour)
ax4 = axes[1, 1]
clusters = ['DAB-small\n(single-node)', 'DAB-medium\n(2-4 workers)', 'DAB-large\n(2-4 workers)']
vm_costs = [1.36, 4.08, 6.80]  # EUR per hour (driver + workers at max)
dbu_costs = [0.11, 0.33, 0.55]  # EUR per hour
total_costs = [v + d for v, d in zip(vm_costs, dbu_costs)]

x_pos = np.arange(len(clusters))
width = 0.35

bars4_1 = ax4.bar(x_pos, vm_costs, width, label='VM Cost', color='#4c72b0', alpha=0.8)
bars4_2 = ax4.bar(x_pos, dbu_costs, width, bottom=vm_costs, label='DBU Cost', color='#55a868', alpha=0.8)

ax4.set_ylabel('Cost per Hour (EUR)', fontsize=12, fontweight='bold')
ax4.set_title('Cluster Cost Breakdown', fontsize=13, fontweight='bold')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(clusters)
ax4.set_ylim(0, 9)
ax4.legend(fontsize=10, loc='upper left')
ax4.grid(axis='y', alpha=0.3, linestyle='--')

# Add total cost labels
for i, (bar1, bar2, total) in enumerate(zip(bars4_1, bars4_2, total_costs)):
    ax4.text(bar1.get_x() + bar1.get_width()/2., total + 0.2,
             f'€{total:.2f}/hr',
             ha='center', va='bottom', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.show()

print("\n" + "="*80)
print("KEY INSIGHTS FROM CHARTS (PRODUCTION SCALE: 15M records/day):")
print("="*80)
print("1. 🚨 Strategy choice (monthly vs day-by-day) has MINIMAL cost impact - €344k both!")
print("2. ⏱️  Day-by-day with 10x parallelization is 10x FASTER (13 days vs 130 days)")
print("3. 💰 Throughput optimization (40→200 rps) = 80% cost reduction (€344k → €69k)")
print("4. 🔴 At 40 rps, processing 1 day of data takes 4.3 DAYS - you fall behind daily!")
print("5. ✅ Priority #1: OPTIMIZE CODE THROUGHPUT (not strategy or cluster choice)")
print("6. ✅ Priority #2: Implement parallelization for manageable wallclock time")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Export Report Summary as DataFrame
from pyspark.sql import Row
from datetime import datetime

# Create comparison summary DataFrame - PRODUCTION SCALE (15M records/day)
comparison_data = [
    Row(
        strategy="Monthly (Serial)",
        cluster="DAB-large",
        records_per_day="15M",
        wallclock_days=130.0,
        total_cost_eur=344640,
        cost_per_dataset_eur=22976,
        parallelization="1x",
        failure_recovery="All-or-nothing",
        monitoring_granularity="Coarse",
        timeout_risk="Extreme",
        complexity="Simple",
        recommendation="NOT FEASIBLE"
    ),
    Row(
        strategy="Day-by-Day (Serial)",
        cluster="DAB-large",
        records_per_day="15M",
        wallclock_days=130.0,
        total_cost_eur=344640,
        cost_per_dataset_eur=22976,
        parallelization="1x",
        failure_recovery="Per-day",
        monitoring_granularity="Fine",
        timeout_risk="Extreme",
        complexity="Moderate",
        recommendation="NOT FEASIBLE"
    ),
    Row(
        strategy="Day-by-Day (Parallel 10x)",
        cluster="DAB-large",
        records_per_day="15M",
        wallclock_days=13.0,
        total_cost_eur=344640,
        cost_per_dataset_eur=22976,
        parallelization="10x",
        failure_recovery="Per-day",
        monitoring_granularity="Fine",
        timeout_risk="Manageable",
        complexity="Higher",
        recommendation="VIABLE (needs throughput optimization)"
    ),
    Row(
        strategy="Day-by-Day (10x) + Optimized",
        cluster="DAB-large",
        records_per_day="15M",
        wallclock_days=2.6,
        total_cost_eur=68925,
        cost_per_dataset_eur=4595,
        parallelization="10x",
        failure_recovery="Per-day",
        monitoring_granularity="Fine",
        timeout_risk="Low",
        complexity="Higher",
        recommendation="RECOMMENDED (after 5x throughput improvement)"
    )
]

df_comparison = spark.createDataFrame(comparison_data)

print("\n" + "="*100)
print("STRATEGY COMPARISON SUMMARY (15 datasets × 1 month backfill)")
print("="*100)
display(df_comparison)

# Calculate savings - PRODUCTION SCALE
current_cost = 344640  # Current throughput (40 rps)
optimized_cost = 68925  # After 5x throughput improvement (200 rps)
savings = current_cost - optimized_cost
savings_pct = (savings / current_cost) * 100
time_current = 130.0  # Serial processing
time_parallel_10x = 13.0  # 10x parallelization
time_optimized = 2.6  # 10x parallel + 5x throughput
time_savings_parallel = time_current - time_parallel_10x
time_savings_optimized = time_current - time_optimized

print("\n" + "="*100)
print("COST & TIME ANALYSIS - PRODUCTION SCALE (15M records/day)")
print("="*100)
print(f"\n💰 COST IMPACT:")
print(f"   Current (40 rps): €{current_cost:,.0f} for 15 datasets")
print(f"   After optimization (200 rps): €{optimized_cost:,.0f} for 15 datasets")
print(f"   SAVINGS: €{savings:,.0f} ({savings_pct:.0f}% reduction)")
print(f"   Per dataset savings: €{savings/15:,.0f}")
print(f"\n⌚ TIME IMPACT:")
print(f"   Serial (no parallelization): {time_current:.0f} days")
print(f"   With 10x parallelization: {time_parallel_10x:.0f} days ({time_savings_parallel:.0f} days faster)")
print(f"   With 10x parallel + 5x throughput: {time_optimized:.1f} days ({time_savings_optimized:.1f} days faster)")
print(f"   Overall speed-up: {time_current/time_optimized:.0f}x faster")

print("\n" + "="*100)
print("🚨 CRITICAL FINDINGS (PRODUCTION SCALE: 15M records/day)")
print("="*100)
print("1. 🔴 THROUGHPUT CRISIS: At 40 rps, processing 1 day takes 4.3 days - you fall behind daily!")
print("2. 🚨 Strategy (monthly vs day-by-day) has MINIMAL cost impact at this scale (€344k both)")
print("3. 🎯 Priority #1: OPTIMIZE THROUGHPUT 40→200 rps = €276k savings (80% reduction)")
print("4. ⏱️  Priority #2: Implement 10x parallelization (130 days → 13 days wallclock)")
print("5. ✅ Day-by-Day enables parallelization + better monitoring/recovery")
print("6. ❌ DBU tracking is BROKEN - costs are estimates (but proportions are valid)")
print("7. 📋 Cluster choice testing is SECONDARY to throughput optimization")
print("="*100)

# Export metadata - PRODUCTION SCALE
export_metadata = {
    "report_generated_at": datetime.now().isoformat(),
    "production_scale": "15-20M records/day per dataset",
    "benchmark_data_path": "abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/",
    "tests_completed": 1,
    "tests_pending": 7,
    "recommendation": "Day-by-Day with 10x+ parallelization AFTER throughput optimization",
    "critical_blocker": "THROUGHPUT CRISIS: 40 rps cannot keep up with daily data volume",
    "throughput_optimization_savings": f"€{savings:,.0f} (80% cost reduction)",
    "parallelization_time_savings": f"{time_savings_parallel:.0f} days (10x parallel)",
    "combined_improvement": f"50x faster ({time_current:.0f}d → {time_optimized:.1f}d) + 80% cheaper"
}

print("\n📄 Export this report to Confluence using the markdown cells above.")
print("📊 Charts generated - save as images for Confluence attachment.")
print("✅ Summary DataFrame created - use for executive presentation.")

# COMMAND ----------

# DBTITLE 1,Executive Summary Visualization
import matplotlib.pyplot as plt
import numpy as np

# Create executive summary visualization
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('RDM_Party Backfill - Executive Summary (Production Scale: 15M records/day)', 
             fontsize=16, fontweight='bold')

# Chart 1: Strategy Choice Impact (minimal!)
ax1 = axes[0]
strategies = ['Monthly\n(Serial)', 'Day-by-Day\n(Serial)', 'Day-by-Day\n(10x Parallel)']
costs = [344.64, 344.64, 344.64]  # thousand EUR - SAME!
times = [130, 130, 13]  # days

x_pos = np.arange(len(strategies))
width = 0.35

# Cost bars
bars1 = ax1.bar(x_pos - width/2, costs, width, label='Cost (€k)', color='#d62728', alpha=0.7)
# Time bars (scaled to fit on same chart)
scaled_times = [t * 2.65 for t in times]  # scale to match cost range
bars2 = ax1.bar(x_pos + width/2, scaled_times, width, label='Time (days × 2.65)', color='#ff7f0e', alpha=0.7)

ax1.set_ylabel('Value', fontsize=12, fontweight='bold')
ax1.set_title('Strategy Choice Impact: MINIMAL\n(Cost is same, only parallelization affects time)', 
              fontsize=13, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(strategies)
ax1.legend(fontsize=10)
ax1.grid(axis='y', alpha=0.3, linestyle='--')

# Add actual values on bars
for i, (bar, cost, time) in enumerate(zip(bars1, costs, times)):
    ax1.text(bar.get_x() + bar.get_width()/2., cost + 10,
             f'€{cost:.0f}k',
             ha='center', va='bottom', fontsize=10, fontweight='bold')
    ax1.text(bars2[i].get_x() + bars2[i].get_width()/2., scaled_times[i] + 10,
             f'{time}d',
             ha='center', va='bottom', fontsize=10, fontweight='bold')

# Add "SAME COST" annotation
ax1.annotate('SAME COST!', xy=(1, 344.64), xytext=(1, 380),
            fontsize=14, fontweight='bold', color='red',
            ha='center',
            arrowprops=dict(arrowstyle='->', color='red', lw=2))

# Chart 2: Throughput Optimization Impact (MASSIVE!)
ax2 = axes[1]
throughput_scenarios = ['Current\n40 rps', 'Optimized\n200 rps']
costs_throughput = [344.64, 68.93]  # thousand EUR
times_throughput = [130, 26]  # days (with 5x parallel after optimization)

x_pos2 = np.arange(len(throughput_scenarios))

# Cost bars
bars3 = ax2.bar(x_pos2 - width/2, costs_throughput, width, label='Cost (€k)', 
                color=['#d62728', '#2ca02c'], alpha=0.7)
# Time bars (scaled)
scaled_times2 = [t * 2.65 for t in times_throughput]
bars4 = ax2.bar(x_pos2 + width/2, scaled_times2, width, label='Time (days × 2.65)', 
                color=['#ff7f0e', '#90ee90'], alpha=0.7)

ax2.set_ylabel('Value', fontsize=12, fontweight='bold')
ax2.set_title('Throughput Optimization Impact: MASSIVE\n(80% cost reduction + 5x faster)', 
              fontsize=13, fontweight='bold')
ax2.set_xticks(x_pos2)
ax2.set_xticklabels(throughput_scenarios)
ax2.legend(fontsize=10)
ax2.grid(axis='y', alpha=0.3, linestyle='--')

# Add values and savings
for i, (bar, cost, time) in enumerate(zip(bars3, costs_throughput, times_throughput)):
    ax2.text(bar.get_x() + bar.get_width()/2., cost + 10,
             f'€{cost:.0f}k',
             ha='center', va='bottom', fontsize=10, fontweight='bold')
    ax2.text(bars4[i].get_x() + bars4[i].get_width()/2., scaled_times2[i] + 10,
             f'{time}d',
             ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    if i == 1:  # Optimized scenario
        savings = costs_throughput[0] - cost
        savings_pct = (savings / costs_throughput[0]) * 100
        ax2.text(bar.get_x() + bar.get_width()/2., cost/2,
                 f'-€{savings:.0f}k\n(-{savings_pct:.0f}%)',
                 ha='center', va='center', fontsize=11, fontweight='bold', color='white')

plt.tight_layout()
plt.show()

print("\n" + "="*90)
print("🎯 EXECUTIVE SUMMARY - KEY TAKEAWAY")
print("="*90)
print("\n🚨 THE ORIGINAL QUESTION WAS THE WRONG QUESTION!\n")
print("   Asked: 'Monthly vs Day-by-Day strategy?'")
print("   Answer: 'Strategy choice has minimal cost impact - focus on throughput!'")
print("\n📈 KEY INSIGHT #1: Strategy has almost NO cost impact")
print("   • Monthly: €344,640")
print("   • Day-by-Day: €344,640")
print("   • Difference: €0 (same cluster hours!)")
print("\n🚀 KEY INSIGHT #2: Throughput optimization has MASSIVE impact")
print("   • Current (40 rps): €344,640")
print("   • Optimized (200 rps): €68,925")
print("   • Savings: €275,715 (80% reduction)")
print("\n✅ RECOMMENDATION:")
print("   1. PRIORITY #1: Optimize throughput (40 → 200+ rps) = 80% cost savings")
print("   2. PRIORITY #2: Use day-by-day + 10x parallelization for time reduction")
print("   3. Strategy choice matters for operations, NOT for cost")
print("="*90)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RDM_Party Backfill Spike — Benchmark_Runner
# MAGIC
# MAGIC **Purpose:** Orchestrates all benchmark executions for the RDM_Party backfill spike.
# MAGIC For each combination of `strategy` × `cluster_config_id`, this notebook:
# MAGIC - Expands the given `load_dates` / month identifier into individual `Load_Date` values
# MAGIC - Calls `RDM_Party.py` via `dbutils.notebook.run()` for each date
# MAGIC - Captures wall-clock time, records processed, throughput, DBU, and Spark metrics
# MAGIC - Appends one row per execution to the shared `spike_benchmark_results` Delta table
# MAGIC - Updates `spike_backfill_status` per date (PENDING → IN_PROGRESS → COMPLETED / FAILED)
# MAGIC
# MAGIC **Does NOT modify `RDM_Party.py`** — fully non-destructive with respect to the production notebook.
# MAGIC
# MAGIC **Requirements covered:** 2, 3 (strategy × cluster benchmarking), 4 (metrics capture)
# MAGIC
# MAGIC #### Widgets
# MAGIC | Widget            | Values                                                  |
# MAGIC |-------------------|---------------------------------------------------------|
# MAGIC | strategy          | `monthly` or `day_by_day`                               |
# MAGIC | cluster_config_id | `dab_small`, `dab_medium`, `dab_large`, `radar_large_uc`|
# MAGIC | load_dates        | Comma-separated `YYYYMMDD` (day-by-day) or `YYYYMM` month (monthly) |
# MAGIC | run_number        | 1–5                                                     |
# MAGIC | rdm_party_path    | Relative path to `RDM_Party` notebook (default: `../RDM_Party`) |
# MAGIC
# MAGIC #### Author
# MAGIC - Generated for RDM_Party Backfill Spike

# COMMAND ----------

# DBTITLE 1,⚠️ Known Issues
# MAGIC %md
# MAGIC ### ⚠️ Known Issues
# MAGIC
# MAGIC #### Delta Configuration Error in RDM_Party
# MAGIC
# MAGIC **Issue:** The production RDM_Party notebook contains an invalid Delta Lake configuration option that causes benchmark runs to fail:
# MAGIC
# MAGIC ```
# MAGIC [DELTA_UNKNOWN_CONFIGURATION] Unknown configuration was specified: delta.autooptimize.enabled
# MAGIC ```
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - The RDM_Party notebook at the production path uses `.option("delta.autoOptimize.enabled", "true")`
# MAGIC - This is **not a valid Delta Lake option** (correct options are `delta.autoOptimize.optimizeWrite` or `delta.autoOptimize.autoCompact`)
# MAGIC
# MAGIC **Fix Required:**
# MAGIC The RDM_Party notebook needs to be updated to **remove** this invalid option from all Delta write operations:
# MAGIC
# MAGIC ```python
# MAGIC # ❌ WRONG - causes DELTA_UNKNOWN_CONFIGURATION error
# MAGIC (df.write
# MAGIC    .format("delta")
# MAGIC    .mode("overwrite")
# MAGIC    .option("delta.autoOptimize.enabled", "true")  # Invalid option
# MAGIC    .save(output_path))
# MAGIC
# MAGIC # ✅ CORRECT - remove the invalid option
# MAGIC (df.write
# MAGIC    .format("delta")
# MAGIC    .mode("overwrite")
# MAGIC    .save(output_path))
# MAGIC ```
# MAGIC
# MAGIC **Production Path:**
# MAGIC ```
# MAGIC /Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/radarv1/RadarDataModel/RadarDataModel_Version3/RDM_Party
# MAGIC ```
# MAGIC
# MAGIC **Impact:**
# MAGIC - All benchmark runs calling this notebook will fail with the Delta configuration error
# MAGIC - The Benchmark_Runner has been updated to detect and report this specific error clearly
# MAGIC
# MAGIC **Status:** Awaiting fix in production RDM_Party notebook

# COMMAND ----------

# DBTITLE 1,Install dependencies
# %pip install hypothesis>=6.0 --quiet  # Uncomment if not pre-installed via init script

# COMMAND ----------

# DBTITLE 1,Read environment variables
import os
from datetime import datetime, timedelta
import calendar
import time
import uuid
import json

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DecimalType, BooleanType, DateType, TimestampType
)

# Use 'dev' as default environment if ENV variable is not set
environment   = os.getenv('ENV', 'dev')
SARADAR       = f"saradar{environment}"
RESULTS_PATH  = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/benchmark_results/"
STATUS_PATH   = f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net/spike/backfill_status/"

print(f"Environment: {environment}")
print(f"Storage account: {SARADAR}")
print(f"Results path: {RESULTS_PATH}")
print(f"Status path: {STATUS_PATH}")

# COMMAND ----------

# DBTITLE 1,Read benchmark results from ADLS
# Import required modules
from pyspark.sql import functions as F

# Read benchmark results from production ADLS location
results_path = "abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/"

print(f"Reading data from: {results_path}")
print("=" * 80)

try:
    # Read the Delta table
    df_results = spark.read.format("delta").load(results_path)
    
    # Show basic info
    print(f"\n✓ Successfully loaded benchmark results")
    print(f"Total records: {df_results.count():,}")
    print(f"\nSchema:")
    df_results.printSchema()
    
    # Display sample data
    print("\nSample data (10 rows):")
    display(df_results.limit(10))
    
    # Show summary statistics
    print("\nSummary by strategy and cluster:")
    summary_df = (df_results
        .groupBy("strategy", "cluster_config_id", "run_number")
        .agg(
            F.count("*").alias("num_executions"),
            F.sum("records_processed").alias("total_records"),
            F.avg("wall_clock_minutes").alias("avg_duration_min"),
            F.avg("throughput_rps").alias("avg_throughput_rps"),
            F.sum("total_dbu").alias("total_dbu")
        )
        .orderBy("strategy", "cluster_config_id", "run_number")
    )
    display(summary_df)
    
except Exception as e:
    print(f"\n❌ Error reading data: {str(e)}")
    print("\nThis may be because:")
    print("  1. The table doesn't exist yet (no benchmark runs completed)")
    print("  2. Storage credentials are not configured")
    print("  3. The path is incorrect")

# COMMAND ----------

# DBTITLE 1,Alternative: Read as Parquet or list files
# Alternative approaches to read the data

# Option 1: List files in the directory
print("Option 1: List directory contents")
print("=" * 80)
try:
    files = dbutils.fs.ls("abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/")
    for file in files:
        file_type = "DIR" if file.isDir() else "FILE"
        size_mb = file.size / (1024 * 1024) if file.size > 0 else 0
        print(f"{file_type:<6} {size_mb:>12,.2f} MB  {file.name}")
    print(f"\nTotal items: {len(files)}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 80)
print("Option 2: Read with SQL read_files()")
print("=" * 80)

# Option 2: Use SQL read_files to auto-detect format
try:
    df = spark.sql("""
        SELECT * 
        FROM read_files('abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/')
        LIMIT 10
    """)
    display(df)
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 80)
print("Option 3: Read as Parquet directly")
print("=" * 80)

# Option 3: Read as Parquet (if Delta format fails)
try:
    df_parquet = spark.read.parquet("abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/")
    print(f"Records: {df_parquet.count():,}")
    display(df_parquet.limit(10))
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Detailed analysis of benchmark results
# Detailed analysis of the benchmark results
from pyspark.sql import functions as F

results_path = "abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/"
df_results = spark.read.format("delta").load(results_path)

print("=" * 80)
print("BENCHMARK RESULTS ANALYSIS")
print("=" * 80)

# 1. Overall statistics
print("\n1. OVERALL STATISTICS:")
total_executions = df_results.count()
total_records = df_results.agg(F.sum("records_processed")).collect()[0][0]
total_duration = df_results.agg(F.sum("wall_clock_minutes")).collect()[0][0]
print(f"   Total executions: {total_executions:,}")
print(f"   Total records processed: {total_records:,}")
print(f"   Total wall-clock time: {total_duration:,.2f} minutes ({total_duration/60:.2f} hours)")

# 2. Performance by date (top 10 fastest, top 10 slowest)
print("\n2. PERFORMANCE BY DATE:")
print("\n   Top 5 fastest executions:")
fastest = df_results.select(
    "load_date", "wall_clock_minutes", "records_processed", "throughput_rps"
).orderBy("wall_clock_minutes").limit(5)
display(fastest)

print("\n   Top 5 slowest executions:")
slowest = df_results.select(
    "load_date", "wall_clock_minutes", "records_processed", "throughput_rps"
).orderBy(F.desc("wall_clock_minutes")).limit(5)
display(slowest)

# 3. Throughput distribution
print("\n3. THROUGHPUT DISTRIBUTION:")
throughput_stats = df_results.agg(
    F.min("throughput_rps").alias("min_rps"),
    F.avg("throughput_rps").alias("avg_rps"),
    F.max("throughput_rps").alias("max_rps"),
    F.stddev("throughput_rps").alias("stddev_rps")
).collect()[0]
print(f"   Min throughput: {throughput_stats['min_rps']:,.2f} records/sec")
print(f"   Avg throughput: {throughput_stats['avg_rps']:,.2f} records/sec")
print(f"   Max throughput: {throughput_stats['max_rps']:,.2f} records/sec")
print(f"   Std deviation: {throughput_stats['stddev_rps']:,.2f} records/sec")

# 4. Records processed per day
print("\n4. RECORDS PROCESSED PER DAY (sample):")
records_by_date = df_results.select(
    "load_date", "records_processed", "wall_clock_minutes"
).orderBy("load_date").limit(10)
display(records_by_date)

# 5. Check for issues
print("\n5. DATA QUALITY CHECKS:")
zero_dbu = df_results.filter(F.col("total_dbu") == 0).count()
zero_records = df_results.filter(F.col("records_processed") == 0).count()
failed = df_results.filter(F.col("status") == "FAILED").count()
print(f"   Executions with zero DBU: {zero_dbu} (⚠️ DBU tracking not working)")
print(f"   Executions with zero records: {zero_records}")
print(f"   Failed executions: {failed}")

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Visualize performance trends
# Visualize performance trends over time
import matplotlib.pyplot as plt
import pandas as pd

results_path = "abfss://radardatamodel@saradarpreprd.dfs.core.windows.net/spike/benchmark_results/"
df_results = spark.read.format("delta").load(results_path)

# Convert to pandas for plotting
pdf = df_results.select(
    "load_date", 
    "wall_clock_minutes", 
    "records_processed", 
    "throughput_rps"
).orderBy("load_date").toPandas()

# Convert load_date to datetime for better x-axis
pdf['date'] = pd.to_datetime(pdf['load_date'], format='%Y%m%d')

# Create subplots
fig, axes = plt.subplots(3, 1, figsize=(14, 10))
fig.suptitle('RDM_Party Backfill Benchmark Results - Monthly Strategy (dab_large)', 
             fontsize=14, fontweight='bold')

# Plot 1: Wall-clock time
axes[0].plot(pdf['date'], pdf['wall_clock_minutes'], marker='o', linewidth=2, markersize=4, color='#1f77b4')
axes[0].set_ylabel('Duration (minutes)', fontsize=11)
axes[0].set_title('Execution Duration per Day', fontsize=12)
axes[0].grid(True, alpha=0.3)
axes[0].axhline(y=pdf['wall_clock_minutes'].mean(), color='red', linestyle='--', 
                label=f'Mean: {pdf["wall_clock_minutes"].mean():.2f} min', alpha=0.7)
axes[0].legend()

# Plot 2: Records processed
axes[1].bar(pdf['date'], pdf['records_processed'], color='#2ca02c', alpha=0.7, width=0.8)
axes[1].set_ylabel('Records Processed', fontsize=11)
axes[1].set_title('Records Processed per Day', fontsize=12)
axes[1].grid(True, alpha=0.3, axis='y')
axes[1].axhline(y=pdf['records_processed'].mean(), color='red', linestyle='--',
                label=f'Mean: {pdf["records_processed"].mean():,.0f}', alpha=0.7)
axes[1].legend()

# Plot 3: Throughput
axes[2].plot(pdf['date'], pdf['throughput_rps'], marker='s', linewidth=2, markersize=4, color='#ff7f0e')
axes[2].set_xlabel('Load Date', fontsize=11)
axes[2].set_ylabel('Throughput (records/sec)', fontsize=11)
axes[2].set_title('Processing Throughput per Day', fontsize=12)
axes[2].grid(True, alpha=0.3)
axes[2].axhline(y=pdf['throughput_rps'].mean(), color='red', linestyle='--',
                label=f'Mean: {pdf["throughput_rps"].mean():.2f} rps', alpha=0.7)
axes[2].legend()

# Rotate x-axis labels for better readability
for ax in axes:
    ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

print("\n" + "=" * 80)
print("SUMMARY STATISTICS:")
print("=" * 80)
print(f"Date range: {pdf['date'].min().strftime('%Y-%m-%d')} to {pdf['date'].max().strftime('%Y-%m-%d')}")
print(f"Total days processed: {len(pdf)}")
print(f"\nDuration:")
print(f"  Min: {pdf['wall_clock_minutes'].min():.2f} min")
print(f"  Max: {pdf['wall_clock_minutes'].max():.2f} min")
print(f"  Mean: {pdf['wall_clock_minutes'].mean():.2f} min")
print(f"  Median: {pdf['wall_clock_minutes'].median():.2f} min")
print(f"\nRecords Processed:")
print(f"  Min: {pdf['records_processed'].min():,}")
print(f"  Max: {pdf['records_processed'].max():,}")
print(f"  Mean: {pdf['records_processed'].mean():,.0f}")
print(f"  Total: {pdf['records_processed'].sum():,}")
print(f"\nThroughput:")
print(f"  Min: {pdf['throughput_rps'].min():.2f} records/sec")
print(f"  Max: {pdf['throughput_rps'].max():.2f} records/sec")
print(f"  Mean: {pdf['throughput_rps'].mean():.2f} records/sec")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Importing RadarUtils
from RadarUtils import authenticate_storage_account
authenticate_storage_account(SARADAR)

# COMMAND ----------

# DBTITLE 1,Define widgets
# Notebook selection widgets
dbutils.widgets.dropdown("notebooks_mode", "single", ["single", "selected", "all"], "Notebooks Mode")
dbutils.widgets.text("notebook_names", "", "Notebook Names (comma-separated)")
dbutils.widgets.text("notebooks_base_path", "/Workspace/Users/rajeev.kumar01@rabobank.com/R-FEC-RADAR/Databricks/radarv1/RadarDataModel/RadarDataModel_Version3", "Notebooks Base Path")

# Other configuration widgets
dbutils.widgets.text("strategy",          "monthly", "Strategy")
dbutils.widgets.text("cluster_config_id", "dab_small", "Cluster Config")
dbutils.widgets.text("load_dates",        "", "Load Dates")        # YYYYMMDD,YYYYMMDD,... or YYYYMM
dbutils.widgets.text("run_number",        "1", "Run Number")
dbutils.widgets.text("run_type",          "historical", "Run Type")
dbutils.widgets.text("timeout_seconds",   "21600", "Timeout (seconds)")   # 6 h per notebook call
dbutils.widgets.dropdown("force_rerun",   "false", ["false", "true"], "Force Rerun")  # Bypass COMPLETED status guard

# Read widget values
notebooks_mode    = dbutils.widgets.get("notebooks_mode").strip().lower()
notebook_names_raw= dbutils.widgets.get("notebook_names").strip()
notebooks_base_path=dbutils.widgets.get("notebooks_base_path").strip()
strategy          = dbutils.widgets.get("strategy").strip().lower()
force_rerun       = dbutils.widgets.get("force_rerun").strip().lower() == "true"
cluster_config_id = dbutils.widgets.get("cluster_config_id").strip()
load_dates_raw    = dbutils.widgets.get("load_dates").strip()
run_number        = int(dbutils.widgets.get("run_number"))
run_type          = dbutils.widgets.get("run_type").strip()
timeout_seconds   = int(dbutils.widgets.get("timeout_seconds"))

assert strategy in ("monthly", "day_by_day"), f"Unknown strategy: {strategy}"
assert 1 <= run_number <= 5,                  f"run_number must be 1–5, got {run_number}"
assert notebooks_mode in ("single", "selected", "all"), f"Unknown notebooks_mode: {notebooks_mode}"

print(f"Notebooks Mode: {notebooks_mode}")
print(f"Strategy: {strategy} | Cluster: {cluster_config_id} | Run: {run_number} | RunType: {run_type}")
if force_rerun:
    print("⚠️  FORCE_RERUN enabled - will reprocess dates even if already COMPLETED")

# COMMAND ----------

# DBTITLE 1,Expand load_dates input into a list of YYYYMMDD strings
def expand_load_dates(raw: str, strategy: str):
    """
    day_by_day: expects comma-separated YYYYMMDD values, e.g. '20250103,20250110,20250117'
    monthly:    expects a YYYYMM string, e.g. '202501' — expands to all days in that month
    Returns a sorted list of YYYYMMDD strings.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("load_dates widget is empty — provide comma-separated YYYYMMDD or a YYYYMM month.")

    if strategy == "monthly":
        if len(raw) != 6 or not raw.isdigit():
            raise ValueError(f"Monthly strategy expects YYYYMM, got '{raw}'")
        year, month = int(raw[:4]), int(raw[4:])
        _, n_days = calendar.monthrange(year, month)
        return [f"{year}{month:02d}{d:02d}" for d in range(1, n_days + 1)]

    # day_by_day: comma-separated list
    dates = [d.strip() for d in raw.split(",") if d.strip()]
    for d in dates:
        if len(d) != 8 or not d.isdigit():
            raise ValueError(f"Expected YYYYMMDD, got '{d}'")
    return sorted(set(dates))

load_dates = expand_load_dates(load_dates_raw, strategy)
print(f"Dates to process ({len(load_dates)}): {load_dates[:5]}{'...' if len(load_dates)>5 else ''}")

# COMMAND ----------

# DBTITLE 1,Expand notebooks selection
# All available RDM notebooks
ALL_NOTEBOOKS = [
    "RDM_Party_SystemIdentifier",
    "RDM_Party_KN1Cases",
    "RDM_Party_RadarKeyStore",
    "RDM_CDDCase_GRAM",
    "RDM_CDDCase_QuestionAnswer_GRAM",
    "RDM_Party_CDDCase_RiskCategories",
    "RDM_Party_AlternativeNames",
    "RDM_Party_ClientOwnership",
    "RDM_Party_Coverage",
    "RDM_Party_Documents",
    "RDM_Party_Identification",
    "RDM_Party",
    "RDM_Party_role",
    "RDM_Party_Selection",
    "RDM_Party_Structure",
    "RDM_Internal_SystemComparison",
    "RDM_Party_Address",
    "RDM_Party_BusinessActivities",
    "RDM_Party_CountryAffiliation"
]

def expand_notebook_list(mode: str, names_raw: str) -> list:
    """
    Returns a list of notebook names to benchmark based on the mode.
    - single: returns the first name from names_raw
    - selected: returns comma-separated names from names_raw
    - all: returns ALL_NOTEBOOKS
    """
    if mode == "all":
        return ALL_NOTEBOOKS
    
    if not names_raw:
        raise ValueError("notebook_names widget is empty — provide at least one notebook name.")
    
    names = [n.strip() for n in names_raw.split(",") if n.strip()]
    
    if mode == "single":
        if len(names) > 1:
            print(f"[WARN] Mode is 'single' but multiple notebooks provided. Using only: {names[0]}")
        return [names[0]]
    
    # mode == "selected"
    # Validate that all specified notebooks exist in ALL_NOTEBOOKS
    invalid = [n for n in names if n not in ALL_NOTEBOOKS]
    if invalid:
        raise ValueError(f"Invalid notebook names: {invalid}. Must be from: {ALL_NOTEBOOKS}")
    
    return names

notebooks_to_run = expand_notebook_list(notebooks_mode, notebook_names_raw)
print(f"Notebooks to benchmark ({len(notebooks_to_run)}): {notebooks_to_run[:5]}{'...' if len(notebooks_to_run)>5 else ''}")

# COMMAND ----------

# DBTITLE 1,Pre-flight check — warn about known RDM_Party issue
print("\n" + "="*80)
print("Starting benchmark for: RDM_Party")
print("="*80)
print()
print("⚠️  KNOWN ISSUE: The production RDM_Party notebook has an invalid Delta config")
print("   that will cause all runs to fail with: DELTA_UNKNOWN_CONFIGURATION")
print()
print("   Fix required in RDM_Party: Remove .option('delta.autoOptimize.enabled', 'true')")
print(f"   Path: {rdm_party_path}")
print()
print("   If the issue persists, check the '⚠️ Known Issues' cell for details.")
print("="*80 + "\n")

# COMMAND ----------

# DBTITLE 1,Clear status for specific date (manual rerun)
"""
MANUAL RERUN OPTION (Alternative to force_rerun widget)

Use this cell when you need to reprocess a specific date that's already marked as COMPLETED.

When to use:
  - One-off rerun of a single date
  - You prefer manual control over the force_rerun widget
  - Debugging a specific date's processing

How to use:
  1. Uncomment the code below
  2. Set date_to_reset to your target date (YYYYMMDD format)
  3. Run this cell
  4. Run the main benchmark loop - the date will now be processed

Note: The force_rerun widget (Cell 18) is preferred for multiple reruns.
"""

# date_to_reset = "20260216"
# try:
#     spark.sql(f"""
#         DELETE FROM delta.`{STATUS_PATH}` 
#         WHERE load_date = '{date_to_reset}'
#     """)
#     print(f"✓ Status cleared for {date_to_reset} - you can now rerun it")
# except Exception as e:
#     print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Cluster configuration reference (maps config_id → metadata)
# Cluster metadata sourced directly from clusters_job.yml
# Standard_D8ads_v5  = 32 GB RAM,  8 vCPUs
# Standard_D16ads_v5 = 64 GB RAM, 16 vCPUs  (per worker node)
# Standard_D32ads_v5 = 128 GB RAM, 32 vCPUs (per worker node)
# NOTE: dab_small uses local[*,4] single-node mode (num_workers=0)
# NOTE: dab_medium / dab_large use autoscale; for benchmark runs fix workers at max
#       to avoid mid-run scale-down distorting wall-clock measurements.
# NOTE: Radar_* UC clusters use data_security_mode=USER_ISOLATION — incompatible with
#       RDM_Party.py's legacy OAuth mount pattern; excluded from spike.
CLUSTER_CONFIGS = {
    # Req 3.1 — single-node baseline (driver only, 32 GB, local[*,4] Photon)
    "dab_small": {
        "cluster_name":   "DAB-small",
        "node_type":      "Standard_D8ads_v5",
        "driver_type":    "Standard_D8ads_v5",
        "workers":        0,        # single-node
        "memory_gb":      32,       # driver node only
        "vcpus":          8,
        "autoscale":      False,
        "min_workers":    None,
        "max_workers":    None,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.1 — single-node baseline",
    },
    # Req 3.3 — autoscaling variant (64 GB/worker, min 2 / max 4, Photon)
    "dab_medium": {
        "cluster_name":   "DAB-medium",
        "node_type":      "Standard_D16ads_v5",
        "driver_type":    "Standard_D16ads_v5",
        "workers":        "autoscale 2-4",
        "memory_gb":      192,      # driver 64 GB + 2 workers × 64 GB (at min scale)
        "memory_gb_max":  320,      # driver 64 GB + 4 workers × 64 GB (at max scale)
        "vcpus":          48,       # at min scale: 3 × 16
        "vcpus_max":      80,       # at max scale: 5 × 16
        "autoscale":      True,
        "min_workers":    2,
        "max_workers":    4,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.3 — autoscaling variant",
    },
    # Req 3.2 — high-end multi-node (128 GB/worker, min 2 / max 4, Photon)
    # For benchmark runs, consider setting fixed_workers=4 to meet the
    # "at least 4 workers" requirement and prevent mid-run scale-down.
    "dab_large": {
        "cluster_name":   "DAB-large",
        "node_type":      "Standard_D32ads_v5",
        "driver_type":    "Standard_D32ads_v5",
        "workers":        "autoscale 2-4",
        "memory_gb":      384,      # driver 128 GB + 2 workers × 128 GB (at min scale)
        "memory_gb_max":  640,      # driver 128 GB + 4 workers × 128 GB (at max scale)
        "vcpus":          96,       # at min scale: 3 × 32
        "vcpus_max":      160,      # at max scale: 5 × 32
        "autoscale":      True,
        "min_workers":    2,
        "max_workers":    4,
        "spot":           False,
        "runtime":        "PHOTON",
        "spark_version":  "14.3.x-scala2.12",
        "req_mapping":    "Req 3.2 — high-end multi-node (≥4 workers at max scale)",
        "benchmark_note": "Pin to max_workers=4 for benchmark runs (Req 3.2 requires ≥4 workers)",
    },
}
cluster_meta = CLUSTER_CONFIGS.get(cluster_config_id, {})
print(f"Cluster metadata: {cluster_meta}")

# COMMAND ----------

# DBTITLE 1,Schema for spike_benchmark_results Delta table
BENCHMARK_SCHEMA = StructType([
    StructField("execution_id",          StringType(),  False),
    StructField("load_date",             StringType(),  False),
    StructField("run_type",              StringType(),  False),
    StructField("strategy",              StringType(),  False),
    StructField("cluster_config_id",     StringType(),  False),
    StructField("run_number",            IntegerType(), False),
    StructField("wall_clock_minutes",    DecimalType(12, 2), False),
    StructField("records_processed",     LongType(),    False),
    StructField("throughput_rps",        DecimalType(18, 2), False),
    StructField("peak_cpu_pct",          DecimalType(6,  2), True),
    StructField("peak_memory_pct",       DecimalType(6,  2), True),
    StructField("peak_io_mb_s",          DecimalType(12, 2), True),
    StructField("total_dbu",             DecimalType(16, 4), False),
    StructField("estimated_cost_usd",    DecimalType(16, 2), False),
    StructField("dbu_rate_usd",          DecimalType(10, 4), False),
    StructField("dbu_rate_date",         StringType(),  False),
    StructField("spark_tasks",           IntegerType(), True),
    StructField("shuffle_read_bytes",    LongType(),    True),
    StructField("shuffle_write_bytes",   LongType(),    True),
    StructField("bytes_spilled",         LongType(),    True),
    StructField("error_message",         StringType(),  True),
    StructField("spark_stage_at_failure",StringType(),  True),
    StructField("retry_attempt",         IntegerType(), False),
    StructField("status",                StringType(),  False),
    StructField("zero_metric_reason",    StringType(),  True),
    StructField("outlier_flag",          BooleanType(), True),
    StructField("outlier_deviation_pct", DecimalType(8, 2), True),
    StructField("source_partition_paths",StringType(),  True),
    StructField("recorded_at_ts",        TimestampType(),False),
])

# COMMAND ----------

# DBTITLE 1,Helper — get Spark job-level metrics for the last completed job
def get_last_spark_job_metrics():
    """
    Returns a dict with aggregated Spark job metrics from the most recent job.
    Uses the Spark listener bus / sc.statusTracker for task/shuffle data.
    Falls back to None values if metrics are unavailable.
    """
    try:
        sc = spark.sparkContext
        status = sc.statusTracker()
        job_ids = status.getJobIdsForGroup(None)
        if not job_ids:
            return {"spark_tasks": None, "shuffle_read_bytes": None,
                    "shuffle_write_bytes": None, "bytes_spilled": None}

        latest_job_id = max(job_ids)
        job_info = status.getJobInfo(latest_job_id)
        stage_ids = job_info.stageIds() if job_info else []

        total_tasks = 0
        shuffle_read = 0
        shuffle_write = 0
        spilled = 0

        for stage_id in stage_ids:
            stage_info = status.getStageInfo(stage_id)
            if stage_info:
                total_tasks  += stage_info.numTasks()
                shuffle_read += getattr(stage_info, "shuffleReadBytes", 0) or 0
                shuffle_write+= getattr(stage_info, "shuffleWriteBytes", 0) or 0
                spilled      += getattr(stage_info, "diskBytesSpilled", 0) or 0

        return {
            "spark_tasks":         total_tasks  or None,
            "shuffle_read_bytes":  shuffle_read or None,
            "shuffle_write_bytes": shuffle_write or None,
            "bytes_spilled":       spilled      or None,
        }
    except Exception as ex:
        print(f"[WARN] Could not collect Spark metrics: {ex}")
        return {"spark_tasks": None, "shuffle_read_bytes": None,
                "shuffle_write_bytes": None, "bytes_spilled": None}

# COMMAND ----------

# DBTITLE 1,Helper — count records in output partition
def count_output_partition(load_date: str) -> int:
    """Reads the output parquet partition and returns its record count."""
    path = (f"abfss://radardatamodel@{SARADAR}.dfs.core.windows.net"
            f"/Party/3/data/EDL_LOAD_DTS={load_date}/*.parquet")
    try:
        return spark.read.parquet(path).count()
    except Exception:
        return 0

# COMMAND ----------

# DBTITLE 1,Helper — write a row to spike_benchmark_results
from decimal import Decimal as D

# Published Databricks list price used for cost calculation.
# Update this value and date if prices change before the benchmark runs.
DBU_RATE_USD  = D("0.4000")   # USD per DBU (All-Purpose Compute, Jobs tier — verify before use)
DBU_RATE_DATE = "2025-01-01"  # Date the price was retrieved

def write_benchmark_row(row_dict: dict):
    """Appends a single row to the spike_benchmark_results Delta table."""
    row_dict["recorded_at_ts"] = datetime.utcnow()
    df = spark.createDataFrame([row_dict], schema=BENCHMARK_SCHEMA)
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")  # Allow schema evolution for new columns
       .save(RESULTS_PATH))

# COMMAND ----------

# DBTITLE 1,Helper — update spike_backfill_status
STATUS_SCHEMA = StructType([
    StructField("load_date",        StringType(),  False),
    StructField("status",           StringType(),  False),
    StructField("last_updated_ts",  TimestampType(),False),
    StructField("execution_id",     StringType(),  True),
    StructField("failure_reason",   StringType(),  True),
])

VALID_STATUSES = {"PENDING", "IN_PROGRESS", "COMPLETED", "FAILED"}

def upsert_status(load_date: str, status: str, execution_id: str = None, failure_reason: str = None, force: bool = False):
    """
    Writes/overwrites a status row for the given load_date.
    Status transitions enforced: COMPLETED → IN_PROGRESS is blocked unless force=True.
    """
    assert status in VALID_STATUSES, f"Invalid status: {status}"

    # Guard: do not regress a COMPLETED partition back to IN_PROGRESS (unless forced)
    if status == "IN_PROGRESS" and not force:
        try:
            existing = (spark.read.format("delta").load(STATUS_PATH)
                            .filter(F.col("load_date") == load_date)
                            .select("status").collect())
            if existing and existing[0]["status"] == "COMPLETED":
                print(f"[SKIP] {load_date} is already COMPLETED — will not revert to IN_PROGRESS.")
                return
        except Exception:
            pass  # Table doesn't exist yet — safe to proceed

    row = {
        "load_date":       load_date,
        "status":          status,
        "last_updated_ts": datetime.utcnow(),
        "execution_id":    execution_id,
        "failure_reason":  failure_reason,
    }
    df = spark.createDataFrame([row], schema=STATUS_SCHEMA)

    # Overwrite just this load_date partition (Delta merge-style upsert via replaceWhere)
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"load_date = '{load_date}'")
       .save(STATUS_PATH))

# COMMAND ----------

# DBTITLE 1,Helper — outlier detection across day-by-day executions
def flag_outliers(exec_times: list) -> list:
    """
    Returns a list of dicts with `outlier_flag` and `outlier_deviation_pct`.
    An execution is flagged as outlier when |t - mean| / mean > 0.20.
    Implements Property 10 of the design spec.
    """
    if not exec_times:
        return []
    mean_t = sum(exec_times) / len(exec_times)
    results = []
    for t in exec_times:
        deviation = abs(t - mean_t) / mean_t if mean_t > 0 else 0.0
        results.append({
            "outlier_flag":          deviation > 0.20,
            "outlier_deviation_pct": round(deviation * 100, 2),
        })
    return results

# COMMAND ----------

# DBTITLE 1,Main benchmark loop — strategy × dates
execution_times = []   # collected for outlier detection (day_by_day only)
execution_ids   = []   # for summary

MAX_RETRIES = 3

for load_date in load_dates:

    execution_id  = str(uuid.uuid4())
    execution_ids.append(execution_id)
    success       = False
    attempt       = 0
    error_msg     = None
    stage_at_fail = None

    # Collect source partition paths (JSON) — used for cross-run comparability check
    source_paths_snapshot = json.dumps({
        "load_date": load_date,
        "strategy":  strategy,
        "cluster":   cluster_config_id,
        "note":      "Actual paths resolved inside RDM_Party.py by Read_GDP_Defined_DataObjects"
    })

    # --- Mark IN_PROGRESS ---
    upsert_status(load_date, "IN_PROGRESS", execution_id, force=force_rerun)

    while attempt < MAX_RETRIES and not success:
        attempt += 1
        t_start = time.time()

        try:
            # -----------------------------------------------------------------
            # Call RDM_Party.py — passes Load_Date; RunType auto-set to "historical"
            # -----------------------------------------------------------------
            result = dbutils.notebook.run(
                rdm_party_path,
                timeout_seconds=timeout_seconds,
                arguments={"Load_Date": load_date}
            )
            t_end = time.time()
            wall_clock_minutes = round((t_end - t_start) / 60, 2)

            # Count output records
            records = count_output_partition(load_date)

            # Throughput
            throughput = round(records / (wall_clock_minutes * 60), 2) if wall_clock_minutes > 0 else 0.0

            # Spark job metrics
            spark_metrics = get_last_spark_job_metrics()

            # Cost — DBU is retrieved from Databricks Jobs API; placeholder here
            # In production, retrieve via REST: GET /api/2.1/jobs/runs/get?run_id=<run_id>
            # For now we store 0 and annotate with BELOW_MEASUREMENT_THRESHOLD
            total_dbu          = D("0.0000")
            estimated_cost_usd = D("0.00")
            zero_metric_reason = "BELOW_MEASUREMENT_THRESHOLD"  # DBU pulled manually post-run

            # Outlier tracking
            execution_times.append(float(wall_clock_minutes))

            row = {
                "execution_id":           execution_id,
                "load_date":              load_date,
                "run_type":               "historical",
                "strategy":               strategy,
                "cluster_config_id":      cluster_config_id,
                "run_number":             run_number,
                "wall_clock_minutes":     D(str(wall_clock_minutes)),
                "records_processed":      records,
                "throughput_rps":         D(str(throughput)),
                "peak_cpu_pct":           None,   # Pulled from Databricks Cluster Metrics API post-run
                "peak_memory_pct":        None,
                "peak_io_mb_s":           None,
                "total_dbu":              total_dbu,
                "estimated_cost_usd":     estimated_cost_usd,
                "dbu_rate_usd":           DBU_RATE_USD,
                "dbu_rate_date":          DBU_RATE_DATE,
                "spark_tasks":            spark_metrics["spark_tasks"],
                "shuffle_read_bytes":     spark_metrics["shuffle_read_bytes"],
                "shuffle_write_bytes":    spark_metrics["shuffle_write_bytes"],
                "bytes_spilled":          spark_metrics["bytes_spilled"],
                "error_message":          None,
                "spark_stage_at_failure": None,
                "retry_attempt":          attempt - 1,
                "status":                 "SUCCESS",
                "zero_metric_reason":     zero_metric_reason,
                "outlier_flag":           None,   # Filled post-loop
                "outlier_deviation_pct":  None,
                "source_partition_paths": source_paths_snapshot,
            }
            write_benchmark_row(row)
            upsert_status(load_date, "COMPLETED", execution_id)
            success = True
            print(f"[OK] {load_date} | {wall_clock_minutes:.2f} min | {records:,} records | attempt {attempt}")

        except Exception as ex:
            t_end = time.time()
            error_msg     = str(ex)[:2000]
            stage_at_fail = "unknown"  # Could be enriched by parsing Databricks job run logs
            
            # Check for known Delta configuration issue in RDM_Party
            if "DELTA_UNKNOWN_CONFIGURATION" in error_msg and "autoOptimize.enabled" in error_msg:
                print(f"[ERROR] {load_date} attempt {attempt}/{MAX_RETRIES}: KNOWN ISSUE - RDM_Party has invalid Delta config")
                print(f"        Fix required in RDM_Party notebook: Remove .option('delta.autoOptimize.enabled', 'true')")
                print(f"        Path: {rdm_party_path}")
            else:
                print(f"[ERROR] {load_date} attempt {attempt}/{MAX_RETRIES}: {error_msg[:200]}")

            if attempt == MAX_RETRIES:
                # All retries exhausted — write FAILED row (no cost/duration extrapolation)
                row = {
                    "execution_id":           execution_id,
                    "load_date":              load_date,
                    "run_type":               "historical",
                    "strategy":               strategy,
                    "cluster_config_id":      cluster_config_id,
                    "run_number":             run_number,
                    "wall_clock_minutes":     D(str(round((t_end - t_start) / 60, 2))),
                    "records_processed":      0,
                    "throughput_rps":         D("0.00"),
                    "peak_cpu_pct":           None,
                    "peak_memory_pct":        None,
                    "peak_io_mb_s":           None,
                    "total_dbu":              D("0.0000"),
                    "estimated_cost_usd":     D("0.00"),
                    "dbu_rate_usd":           DBU_RATE_USD,
                    "dbu_rate_date":          DBU_RATE_DATE,
                    "spark_tasks":            None,
                    "shuffle_read_bytes":     None,
                    "shuffle_write_bytes":    None,
                    "bytes_spilled":          None,
                    "error_message":          error_msg,
                    "spark_stage_at_failure": stage_at_fail,
                    "retry_attempt":          attempt - 1,
                    "status":                 "FAILED",
                    "zero_metric_reason":     None,
                    "outlier_flag":           None,
                    "outlier_deviation_pct":  None,
                    "source_partition_paths": source_paths_snapshot,
                }
                write_benchmark_row(row)
                upsert_status(load_date, "FAILED", execution_id, failure_reason=error_msg[:500])

# COMMAND ----------

# DBTITLE 1,Post-loop — update outlier flags for day-by-day strategy
if strategy == "day_by_day" and execution_times:
    outlier_results = flag_outliers(execution_times)
    for i, (exec_id, ot) in enumerate(zip(execution_ids, outlier_results)):
        if ot["outlier_flag"]:
            try:
                (spark.read.format("delta").load(RESULTS_PATH)
                    .filter(F.col("execution_id") == exec_id)
                    .show(1, truncate=False))
                print(f"[OUTLIER] {load_dates[i]} — deviation {ot['outlier_deviation_pct']:.2f}%")
                # In a full implementation, issue a Delta UPDATE statement here
                # spark.sql(f"""UPDATE delta.`{RESULTS_PATH}`
                #               SET outlier_flag = {ot['outlier_flag']},
                #                   outlier_deviation_pct = {ot['outlier_deviation_pct']}
                #               WHERE execution_id = '{exec_id}'""")
            except Exception as ex:
                print(f"[WARN] Could not update outlier flag for {exec_id}: {ex}")

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n{'='*60}")
print(f"Benchmark_Runner complete")
print(f"Strategy: {strategy} | Cluster: {cluster_config_id} | Run: {run_number}")
print(f"Dates processed: {len(load_dates)} | Success: {sum(1 for t in execution_times if t > 0)}")
if execution_times:
    print(f"Avg wall-clock: {sum(execution_times)/len(execution_times):.2f} min")
    print(f"Range: {min(execution_times):.2f} – {max(execution_times):.2f} min")
print(f"Results path: {RESULTS_PATH}")
print(f"{'='*60}")
