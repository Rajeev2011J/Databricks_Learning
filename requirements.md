# Requirements Document

## Introduction

This spike investigates the most efficient, scalable, and cost-effective approach for backfilling historical data for the **RDM_Party** dataset within the WR Radar data model on Databricks. The notebook `RDM_Party.py` processes approximately 50 million records per day, ingesting from seven source systems (GCDS, GCOB, Legacy2, GIC, KN1, RANZ, KYCMasterListRegistry) and two internal RadarDataModel objects (Party_SystemIdentifier, Party_KN1Cases). The backfill covers 1 January 2025 to the current date (~215 days, ~10.75 billion records total).

The spike evaluates two loading strategies — a **monthly strategy** (a sequential loop of daily `RDM_Party.py` executions covering all days within a calendar month) and a **day-by-day strategy** (each `RDM_Party.py` execution processes exactly one `Load_Date` before the pipeline advances to the next date) — across multiple Databricks cluster configurations. Findings will inform a data-driven recommendation covering performance, cost, and operational manageability applicable to the 20–25 datasets that follow the same pattern in the WR Radar data model.

---

## Glossary

- **RDM_Party**: The Radar Data Model Party dataset produced by `RDM_Party.py`, representing consolidated party/client data across all WR Radar source systems.
- **Backfill**: The process of regenerating and loading historical data for dates that were not processed during normal daily operations.
- **Load_Date**: A widget parameter accepted by `RDM_Party.py` (and peer notebooks) that specifies the date partition to process, in `YYYYMMDD` format.
- **RunType**: A widget parameter in `RDM_Party.py` set to `"daily"` for normal operation or `"historical"` for backfill runs. Setting `Load_Date` automatically activates `RunType = "historical"`.
- **Monthly Strategy**: A loading strategy in which a sequential loop of `RDM_Party.py` executions processes all days within a calendar month, each execution receiving a distinct `Load_Date`.
- **Day-by-Day Strategy**: A loading strategy in which `RDM_Party.py` is executed once per `Load_Date`, processing approximately 50 million records per execution, before the pipeline advances to the next date.
- **DBU**: Databricks Unit — the billing unit used to calculate compute cost on Databricks.
- **Cluster Configuration**: The Databricks cluster setup (node type, count, autoscaling, spot instances) used to run the notebook.
- **Throughput**: The number of records processed per second during a notebook execution.
- **Spike**: A time-boxed technical investigation to produce a data-driven recommendation, not a production feature.
- **WR Radar Data Model**: The suite of 20–25 standardised datasets, including RDM_Party, produced for the Wealth & Retail risk and compliance reporting domain.
- **GCDS**: Global Client Data System — a primary source system for party/client data.
- **GCOB**: Global Client Onboarding system — a source for case-based client details.
- **Legacy2**: Legacy onboarding source providing historical client records.
- **GIC**: Brazilian client system (Rabobank Brazil) providing party data.
- **KN1**: Brazilian KYC system providing counterparty and risk data.
- **RANZ**: Rabobank Australia and New Zealand source system for party data.
- **KYCMasterListRegistry**: Internal RadarPowerApps dataset providing KYC group and sector team enrichment.
- **Party_SystemIdentifier**: Internal RadarDataModel object providing system identifier data joined during party processing.
- **Party_KN1Cases**: Internal RadarDataModel object providing KN1 case data joined during party processing.
- **Confluence**: The team wiki used for documenting findings and recommendations.
- **Benchmark**: A measured test execution that records defined performance and cost metrics under controlled, repeatable conditions.
- **EDL_LOAD_DTS**: The output partition folder name, formatted as `EDL_LOAD_DTS=YYYYMMDD`, written by `save_to_saradar_storage_account` in `RadarUtils.py`.

---

## Requirements

### Requirement 1: Spike Objectives and Success Criteria

**User Story:** As a data engineering team, we want a time-boxed spike that produces a clear, evidence-based recommendation on backfill strategy and cluster configuration, so that we can execute the RDM_Party historical backfill with confidence and extend the approach to the other 20–25 WR Radar datasets.

#### Acceptance Criteria

1. THE Spike SHALL produce a documented recommendation identifying the preferred loading strategy (monthly or day-by-day) and the preferred Databricks cluster configuration for the RDM_Party backfill, where "preferred" is defined as the strategy and configuration that completes the full 215-day backfill within the stakeholder-approved time and cost budget while satisfying the data correctness requirements stated in criterion 3.

2. THE Spike SHALL produce benchmark results covering all combinations of the two loading strategies (monthly and day-by-day) and at least three cluster configurations (single-node, high-end multi-node, and one autoscaling or spot-instance configuration), with each benchmark result recording wall-clock duration in minutes and estimated cluster cost in EUR or USD.

3. THE Spike SHALL evaluate the day-by-day strategy across the full set of RDM_Party source dependencies (GCDS, GCOB, Legacy2, GIC, KN1, RANZ, KYCMasterListRegistry, Party_SystemIdentifier, Party_KN1Cases), where a successful evaluation requires that all nine dependencies are read without error and the output record count for each dependency matches the expected count derived from the source for the same `Load_Date` partition.

4. WHEN the spike is complete, THE Spike SHALL publish all findings to Confluence in a page containing at minimum: (a) a benchmark results table with duration and cost per tested combination, (b) the final strategy and cluster configuration recommendation with supporting rationale, (c) identified risks or blockers per tested combination, and (d) implementation guidance as defined in criterion 5.

5. THE Spike SHALL produce implementation guidance describing all notebook or pipeline changes required to execute the recommended strategy at scale across the 20–25 WR Radar datasets, including at minimum: identification of any parameterisation changes to `Load_Date` or `RunType` handling, any changes required per source dependency, and an estimated effort in person-days to apply the changes to each remaining dataset.

6. BEFORE benchmark execution begins, THE Spike SHALL obtain written stakeholder approval of the time budget (maximum wall-clock duration in hours for the full 215-day backfill) and the cost budget (maximum total cluster cost in EUR or USD), and SHALL record that approval in the Confluence page defined in criterion 4.

7. WHEN benchmark data is collected, THE Spike SHALL demonstrate that the recommended strategy and cluster configuration can process the full 215-day backfill (~10.75 billion records) within the stakeholder-approved time and cost budgets recorded under criterion 6.

---

### Requirement 2: Loading Strategy Comparison

**User Story:** As a data engineer, I want a controlled comparison of the monthly and day-by-day loading strategies on representative data volumes, so that I can understand the trade-offs in execution time, throughput, failure recovery, and operational control before committing to a full backfill.

#### Acceptance Criteria

1. WHEN benchmarking the monthly strategy, THE Benchmark_Runner SHALL execute `RDM_Party.py` once per day for each day in a representative calendar month of at least 28 days containing at least 20 working days, with `Load_Date` set to each individual date and `RunType` set to `"historical"`, and SHALL record all defined metrics for the full month's sequential execution.

2. WHEN benchmarking the day-by-day strategy, THE Benchmark_Runner SHALL execute `RDM_Party.py` once per representative date, selecting at least 3 representative `Load_Date` values distributed across different weeks, with `Load_Date` set to that date and SHALL record all defined metrics for each individual execution.

3. THE Strategy_Comparison SHALL evaluate both strategies against all seven evaluation dimensions: total execution time, throughput, cluster utilisation, scalability, failure recovery granularity, operational control, and compute cost, where "usable data" for a strategy means at least one complete metric set covering all seven dimensions.

4. IF one strategy's benchmark fails entirely and produces no usable data, THEN THE Strategy_Comparison SHALL complete using only the data from the successful strategy and SHALL document the failure mode, root cause, and all metric values recorded before failure as a finding.

5. WHEN a day-by-day execution fails for a given `Load_Date`, THE Strategy_Comparison SHALL record the failure recovery cost as the record count of the single failed `Load_Date` partition (~50 million records), since each execution writes to an isolated `EDL_LOAD_DTS=YYYYMMDD/` partition via overwrite mode.

6. WHEN a monthly execution fails mid-run, THE Strategy_Comparison SHALL record the failure recovery cost as the sum of record counts for all `Load_Date` partitions processed after the last successfully completed and verified date in the monthly sequence.

7. THE Strategy_Comparison SHALL include an extrapolation of each strategy's total cost and elapsed time to cover the full 215-day backfill scope, calculated by linear scaling from the per-day throughput measured in the benchmark (total_cost = cost_per_day × 215; total_duration = duration_per_day × 215).

8. WHERE the team decides to apply the recommended strategy to additional WR Radar datasets, THE Strategy_Comparison SHALL include a scalability assessment stating: (a) whether the strategy applies unchanged or requires per-dataset modifications, (b) the estimated execution time per dataset per day based on RDM_Party throughput, and (c) any ordering or dependency constraints between datasets.

---

### Requirement 3: Cluster Configuration Assessment

**User Story:** As a data engineer, I want benchmark results for at least three distinct Databricks cluster configurations, so that I can select the configuration that best balances execution speed, resource utilisation, and cost for the backfill.

#### Acceptance Criteria

1. THE Cluster_Assessment SHALL benchmark a single-node cluster configuration as the baseline, where "single-node" means the driver node only with no worker nodes, using the smallest instance type available in the team's target Azure region that provides at least 8 GB of memory.

2. THE Cluster_Assessment SHALL benchmark at least one high-end multi-node cluster configuration, where "high-end" means at least 4 worker nodes with a combined total memory of at least 64 GB.

3. THE Cluster_Assessment SHALL benchmark at least one additional cluster configuration, which SHALL be one of: autoscaling cluster (min 1, max ≥ 4 workers), spot-instance cluster, or a Databricks-optimised compute configuration (e.g., memory-optimised or compute-optimised node type available in the team's target Azure region).

4. THE Cluster_Assessment SHALL use only Databricks-supported instance types available in the team's target Azure region, and each multi-node cluster configuration SHALL include at least one worker node and at least 8 GB of total combined memory across all nodes.

5. THE Cluster_Assessment SHALL record, for each configuration: number of nodes (driver + workers), node type (Azure VM SKU), total memory (GB), total vCPUs, whether autoscaling is enabled (and min/max worker bounds if so), and whether spot instances are used.

6. THE Cluster_Assessment SHALL measure DBU consumption per configuration per strategy variant under test, where a "strategy variant" is one complete benchmark execution of either the monthly or day-by-day strategy.

7. THE Cluster_Assessment SHALL measure wall-clock execution time per configuration per strategy variant, where wall-clock time is measured from the moment the Databricks job is submitted to the moment the job reaches a terminal state (succeeded or failed).

8. THE Cluster_Assessment SHALL calculate the estimated monetary cost per configuration per strategy variant as DBU consumption × the team's applicable DBU rate (EUR or USD per DBU), recording both the DBU rate used and its source.

---

### Requirement 4: Benchmarking and Measurement

**User Story:** As a data engineer, I want repeatable, objective benchmark measurements for every strategy–cluster combination, so that comparisons are fair and the recommendation is supported by reproducible evidence.

#### Acceptance Criteria

1. THE Benchmark_Runner SHALL use identical source data partitions (same `Load_Date` values) across all strategy and cluster configuration combinations to ensure comparability.

2. THE Benchmark_Runner SHALL execute each strategy–cluster combination at least 2 and at most 5 times, and SHALL record the mean and range (max minus min) of each metric across all runs for that combination.

3. THE Benchmark_Runner SHALL record the following metrics for every execution to at least 2 decimal places for time and throughput, 4 decimal places for DBU, and 2 decimal places for monetary cost: total execution time (minutes), total records processed, throughput (records per second), peak CPU utilisation (%), peak memory utilisation (%), peak I/O throughput (MB/s), total DBU consumed, and estimated monetary cost in USD at the published Databricks list price recorded at time of benchmark.

4. IF any metric value is zero, THEN THE Benchmark_Runner SHALL record the zero value and annotate the execution metadata with one of the following reasons: `CACHED_RESULT`, `FREE_TIER`, or `BELOW_MEASUREMENT_THRESHOLD`.

5. WHEN a benchmark run produces an execution error, THE Benchmark_Runner SHALL record the Spark stage name and task index at point of failure, the error message, and the retry attempt number, and SHALL re-run that combination up to 2 additional times (3 total attempts) to obtain a valid measurement.

6. IF all 3 attempts for a strategy–cluster combination fail, THEN THE Benchmark_Runner SHALL mark that combination as `FAILED` in the results table and SHALL not extrapolate cost or duration from it.

7. THE Benchmark_Runner SHALL record Spark job-level metrics (number of tasks, shuffle read bytes, shuffle write bytes, bytes spilled to disk) for each execution to support root-cause analysis of performance differences.

8. WHEN measuring the day-by-day strategy, THE Benchmark_Runner SHALL flag any per-date execution time that deviates more than 20% from the mean execution time for that cluster configuration as an outlier, and SHALL record the flag and deviation percentage in the execution metadata.

9. THE Benchmark_Runner SHALL record the `Load_Date`, `RunType`, cluster configuration identifier, and run number as metadata for every benchmark execution to enable full traceability.

---

### Requirement 5: Data Completeness and Accuracy Verification

**User Story:** As a data engineer, I want verification that each backfill execution produces complete and accurate output, so that I am confident the historical data is correct before recommending a strategy for production use.

#### Acceptance Criteria

1. WHEN `RDM_Party.py` completes execution for a given `Load_Date`, THE Verifier SHALL confirm that the output partition at path `Party/3/data/EDL_LOAD_DTS={Load_Date}/` is non-empty and contains a record count no lower than 80% of the total distinct party identifier count across all contributing source partitions for that date.

2. WHEN `RDM_Party.py` completes execution for a given `Load_Date`, THE Verifier SHALL confirm that the `Application` column in the output contains at least one record for each of the expected source systems (GCDS, GCOB, Legacy2, GIC, KN1, RANZ, KYCMasterListRegistry) for that date, provided a non-empty source partition exists for that system on that date.

3. WHEN a backfill run completes, THE Verifier SHALL confirm that exactly one `EDL_LOAD_DTS={Load_Date}/` folder exists in the output path for each processed `Load_Date`, verifying idempotent overwrite behaviour implemented via `mode("overwrite")` in `save_to_saradar_storage_account`.

4. WHEN the same `Load_Date` is reprocessed after an initial successful run, THE Verifier SHALL confirm that the reprocessed output record count equals the original output record count within a tolerance of 0 records, confirming deterministic output given the same source partitions.

5. WHEN a `Load_Date` partition is written, THE Verifier SHALL confirm that the output schema — column names and data types — matches the expected RDM_Party schema for `radar_datamodel_version_number = 3` by comparing against a stored schema reference for that version.

6. WHEN `RDM_Party.py` completes execution for a given `Load_Date` D, THE Verifier SHALL confirm that re-running `RDM_Party.py` for the same `Load_Date` D using the same source partitions produces an output record count equal to the first execution's record count within a tolerance of 0 records.

7. WHEN all daily partitions in the backfill range have been written, THE Verifier SHALL confirm that the sum of record counts across all individual `EDL_LOAD_DTS` partitions equals the total record count obtained by reading all partitions together and counting, within a tolerance of 0 records.

---

### Requirement 6: Failure Recovery and Operational Control

**User Story:** As a data engineer, I want clearly understood failure recovery behaviour for each strategy, so that I can design a backfill pipeline that minimises rework and enables confident monitoring and re-runs.

#### Acceptance Criteria

1. THE Failure_Analysis SHALL document the minimum recoverable unit for each loading strategy: for the day-by-day strategy, the minimum recoverable unit is a single `Load_Date` partition (~50 million records); for the monthly strategy, the minimum recoverable unit is all `Load_Date` partitions in the month that were written after the last successfully verified date.

2. WHEN a day-by-day execution fails for `Load_Date` D, THE Spike SHALL confirm — by analysis of the `save_to_saradar_storage_account` write semantics in `RadarUtils.py` — that re-running `RDM_Party.py` with `Load_Date = D` overwrites only the `EDL_LOAD_DTS=D/` partition and leaves all other date partitions unchanged, and SHALL document this confirmation with the relevant code reference.

3. WHEN a monthly execution (sequential loop of daily executions) fails mid-run at `Load_Date` D_fail, THE Spike SHALL document the exact re-run procedure: identify the last successfully verified `Load_Date` D_last, then re-execute the loop starting from D_last + 1 day through end of month, relying on `mode("overwrite")` to safely overwrite any partially written partitions.

4. THE Spike SHALL document the write semantics of `RDM_Party.py` by confirming that `save_to_saradar_storage_account` uses `mode("overwrite")` scoped to the `EDL_LOAD_DTS=YYYYMMDD/` partition folder, and SHALL state explicitly that this makes every individual daily execution safe to re-run without risk of duplicate records.

5. THE Spike SHALL define a monitoring approach that produces, for each `Load_Date` in the backfill range, a status of `PENDING`, `IN_PROGRESS`, `COMPLETED`, or `FAILED`, enabling the team to identify failed executions and trigger re-runs by resubmitting the job with the failed `Load_Date` as the widget parameter.

6. THE Spike SHALL document the maximum number of concurrent `RDM_Party.py` executions that can run safely in parallel, assessed by verifying that each execution writes to a distinct `EDL_LOAD_DTS=YYYYMMDD/` partition with no shared mutable state, and SHALL include the recommended concurrency limit as a named parameter (`MAX_PARALLEL_EXECUTIONS`) in the implementation guidance. A value of 1 (sequential-only) is a valid documented outcome if shared source read contention is identified.

---

### Requirement 7: Cost Analysis

**User Story:** As a team lead, I want a cost analysis comparing all strategy–cluster combinations, so that I can approve the backfill budget and select the most cost-effective approach.

#### Acceptance Criteria

1. THE Cost_Analysis SHALL calculate total estimated DBU cost for the full 215-day backfill for each strategy–cluster combination using linear extrapolation: `total_DBU = measured_DBU_per_day × 215`, where `measured_DBU_per_day` is the mean DBU consumed per `Load_Date` execution in the benchmark.

2. THE Cost_Analysis SHALL calculate total estimated wall-clock duration for the full 215-day backfill for each strategy–cluster combination using linear extrapolation: `total_duration = measured_duration_per_day × 215`, where the estimated total duration SHALL be greater than zero and no less than the measured benchmark duration for a single day.

3. THE Cost_Analysis SHALL express cost in EUR or USD (matching the team's cloud billing currency) using the Databricks published list price per DBU for the applicable cluster tier, and SHALL record the price per DBU and the date on which the price was retrieved from the Databricks pricing page.

4. THE Cost_Analysis SHALL identify the strategy–cluster combination with the lowest total estimated cost and the strategy–cluster combination with the shortest total estimated duration, and SHALL explicitly state whether these two combinations are the same or different.

5. WHERE the recommended strategy requires notebook or pipeline changes, THE Cost_Analysis SHALL include a one-time engineering effort estimate expressed in person-days within the range of 0.5 to 30 person-days, with a brief justification for the estimate.

6. THE Cost_Analysis SHALL calculate the projected total cost for extending the recommended strategy to the full set of WR Radar datasets by multiplying the per-dataset cost (using RDM_Party benchmark results as proxy) by both 20 and 25 datasets, producing a cost range that represents the minimum and maximum projection.

7. THE Cost_Analysis SHALL present the full-dataset cost projection as a range (20-dataset scenario vs. 25-dataset scenario) so that the team can assess the worst-case budget impact.

---

### Requirement 8: Documentation and Reporting

**User Story:** As a data engineering team, I want all spike findings documented in Confluence in a structured format, so that stakeholders can review the evidence and the team can reference the recommendation during implementation.

#### Acceptance Criteria

1. WHEN the spike is formally closed, THE Documentation SHALL already be published to Confluence with all mandatory sections completed, meaning the Confluence page URL is recorded in the spike's tracking ticket before the ticket is moved to a closed state.

2. THE Documentation MAY be updated after initial publication to incorporate revisions, additional benchmark runs, or stakeholder feedback, provided each update is recorded with a revision date and a brief change summary at the top of the Confluence page.

3. THE Documentation SHALL include a summary section stating the recommended strategy and cluster configuration with a one-paragraph rationale referencing the specific benchmark metrics that support the recommendation.

4. THE Documentation SHALL include a benchmark results section presenting all measured metrics in a table with one row per strategy–cluster–run combination, including columns for all metrics defined in Requirement 4.

5. THE Documentation SHALL include a cost analysis section presenting extrapolated compute and storage costs and durations for the full 215-day backfill, covering all tested strategy–cluster combinations.

6. THE Documentation SHALL include a failure recovery section documenting: the minimum recoverable unit per strategy, the write semantics of `save_to_saradar_storage_account`, and a step-by-step re-run procedure for the recommended strategy.

7. THE Documentation SHALL include an implementation guidance section containing: (a) all prerequisite environment variables and cluster settings, (b) a numbered step-by-step execution procedure for the recommended strategy, (c) all `Load_Date` and `RunType` parameter values required, and (d) expected output record counts or ranges for each step.

8. THE Documentation SHALL include a scalability section noting how the recommended approach applies to the 20–25 WR Radar datasets beyond RDM_Party.

9. IF the spike uncovers risks or blockers (e.g., source data availability gaps, schema drift, cluster quota limits), THEN THE Documentation SHALL include a risks and mitigations section with one entry per identified risk, each entry stating the risk description, likelihood, impact, and proposed mitigation.

10. WHEN the Documentation is published, THE Documentation SHALL be shared with at least one stakeholder outside the engineering team for review, and the review outcome (approved, approved with comments, or changes requested) SHALL be recorded on the Confluence page within 5 business days of publication.

---

### Requirement 9: Property-Based Correctness Properties

**User Story:** As a data engineer, I want defined correctness properties for the backfill process, so that automated or manual verification can confirm data integrity across all processed dates without examining every record individually.

#### Acceptance Criteria

1. WHEN `RDM_Party.py` is executed for `Load_Date` D and then executed again for the same `Load_Date` D using the same source partitions, THE Verifier SHALL confirm that the output record count of the second execution equals the output record count of the first execution (idempotence property: `count(process(D)) == count(process(D))`).

2. WHEN all daily partitions in a contiguous sub-range [D_start, D_end] have been written, THE Verifier SHALL confirm that the sum of individual partition record counts equals the total record count when reading all partitions in the range together, within a tolerance of 0 records (partition additivity invariant).

3. WHEN `RDM_Party.py` completes execution for `Load_Date` D, THE Verifier SHALL confirm that every record in the output partition `EDL_LOAD_DTS=D/` has the `EDL_LOAD_DTS` field set to D (date assignment invariant).

4. WHEN `RDM_Party.py` completes execution for `Load_Date` D, THE Verifier SHALL confirm that every distinct party identifier value in the output for D originated from one of the nine declared source dependencies (GCDS, GCOB, Legacy2, GIC, KN1, RANZ, KYCMasterListRegistry, Party_SystemIdentifier, Party_KN1Cases) for that date (source coverage invariant).

5. WHEN a written partition for `Load_Date` D is read back and validated against the stored RDM_Party v3 schema reference, THE Verifier SHALL confirm that zero schema violations are returned, where a schema violation is defined as a column name mismatch or a non-nullable column containing a null value (schema stability invariant).

6. WHEN `RDM_Party.py` completes execution for `Load_Date` D, THE Verifier SHALL confirm that the output record count is greater than or equal to the count of distinct party identifier values present in the largest single source partition (by record count) for that date, reflecting that the output is a union across sources after deduplication (lower-bound cardinality property).

7. WHEN the monthly strategy and the day-by-day strategy are both applied to the same set of source partitions for a shared date range, THE Verifier SHALL confirm that the total record count across all daily output partitions produced by the monthly strategy equals the total record count across all daily output partitions produced by the day-by-day strategy, within a tolerance of 0 records (strategy equivalence property).

8. WHEN a correctness property check fails for any `Load_Date` D, THE Verifier SHALL record the failing property name, the `Load_Date`, the expected value, and the actual value, and SHALL mark the affected partition as requiring reprocessing before the backfill for D is considered complete.
