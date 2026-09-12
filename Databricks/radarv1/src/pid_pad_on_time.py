# Databricks notebook source
# MAGIC %md
# MAGIC # PID on time - historical dataset

# COMMAND ----------

# DBTITLE 1,pid_on_time_overview_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pid_on_time_overview_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.ClientCaseInitiationStart AS ClientCaseInitiationStart_old
# MAGIC   , MAX(t2.SignOff) AS SignOff_new
# MAGIC   , MAX(t2.Prework) AS Prework_new
# MAGIC   , datediff(t1.ClientCaseInitiationStart, MAX(t2.Prework)) as DateDiff_PID_Prework
# MAGIC   , t3.ClientCaseInitiationStart AS ClientCaseInitiationStart_new
# MAGIC   , t1.EDL_LoadDate
# MAGIC   -- , t2.CaseReviewType
# MAGIC   -- , t2.CaseId
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC       WHEN DATEADD(DAY, -1, MAX(t2.Prework)) > t1.ClientCaseInitiationStart THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN MAX(t2.Prework) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (t1.ClientCaseInitiationStart <> t3.ClientCaseInitiationStart)
# MAGIC         AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.ClientCaseInitiationStart)
# MAGIC       THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.ClientCaseInitiationStart)
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC         AND (DATEADD(DAY, -1, MAX(t2.Prework)) > MAX(t2.SignOff))
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC     END AS OnTime
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC   AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC   AND t1.Scope = 'backToGreen'
# MAGIC   AND t1.ClientLifeCycleName = 'Client'
# MAGIC   AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC GROUP BY 1,2,3,7,8

# COMMAND ----------

# DBTITLE 1,pid_on_time_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pid_on_time_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC , pid_on_time_overview_cte AS (
# MAGIC     SELECT
# MAGIC       t1.UniqueGcobId
# MAGIC       , t1.SourceClient
# MAGIC       , t1.ClientCaseInitiationStart AS ClientCaseInitiationStart_old
# MAGIC       , MAX(t2.SignOff) AS SignOff_new
# MAGIC       , MAX(t2.Prework) AS Prework_new
# MAGIC       , datediff(t1.ClientCaseInitiationStart, MAX(t2.Prework)) as DateDiff_PID_Prework
# MAGIC       , t3.ClientCaseInitiationStart AS ClientCaseInitiationStart_new
# MAGIC
# MAGIC       , CASE
# MAGIC           WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC           WHEN DATEADD(DAY, -1, MAX(t2.Prework)) > t1.ClientCaseInitiationStart THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN MAX(t2.Prework) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (t1.ClientCaseInitiationStart <> t3.ClientCaseInitiationStart)
# MAGIC             AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.ClientCaseInitiationStart)
# MAGIC           THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.ClientCaseInitiationStart)
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.Prework)) < t1.ClientCaseInitiationStart)
# MAGIC             AND (DATEADD(DAY, -1, MAX(t2.Prework)) > MAX(t2.SignOff))
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC         END AS OnTime
# MAGIC
# MAGIC     FROM radar.clients_historical t1
# MAGIC     LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC     LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC     WHERE
# MAGIC         t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC         AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE)
# MAGIC         AND t1.Scope = 'backToGreen'
# MAGIC         AND t1.ClientLifeCycleName = 'Client'
# MAGIC     GROUP BY 1,2,3,7
# MAGIC   )
# MAGIC
# MAGIC SELECT 
# MAGIC   MONTH(ClientCaseInitiationStart_old) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('On time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('On time or rescheduled') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesOnTimeOrRescheduledCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Not on time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesNotOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('Not on time - to be verified') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesNotOnTimeToBeVerifiedCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(ClientCaseInitiationStart_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(ClientCaseInitiationStart_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Offboarding') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOffboardedCount
# MAGIC
# MAGIC FROM pid_on_time_overview_cte
# MAGIC GROUP BY MONTH(ClientCaseInitiationStart_old)
# MAGIC ORDER BY MONTH(ClientCaseInitiationStart_old) ASC

# COMMAND ----------

# MAGIC %md
# MAGIC # PAD on time - historical dataset

# COMMAND ----------

# DBTITLE 1,pad_on_time_overview_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pad_on_time_overview_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   t1.UniqueGcobId
# MAGIC   , t1.SourceClient
# MAGIC   , t1.CDDExecution AS CDDExecution_old
# MAGIC   , MAX(t2.SignOff) AS SignOff_new
# MAGIC   , MAX(t2.ReadyForAssessment) AS ReadyForAssessment_new
# MAGIC   , datediff(t1.CDDExecution, MAX(t2.ReadyForAssessment)) as DateDiff_CDDExecution_ReadyForAssessment
# MAGIC   , t3.CDDExecution AS CDDExecution_new
# MAGIC   , t1.EDL_LoadDate
# MAGIC
# MAGIC   , CASE
# MAGIC       WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC       WHEN DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > t1.CDDExecution THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN MAX(t2.ReadyForAssessment) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (t1.CDDExecution <> t3.CDDExecution)
# MAGIC         AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.CDDExecution)
# MAGIC       THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.CDDExecution)
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > MAX(t2.SignOff))
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC         AND (MAX(t2.SignOff) > DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)))
# MAGIC         AND (DATEADD(DAY, -1, MAX(t2.SignOff)) < t1.CDDExecution) -- handle when ready and sign off are both before cddexecution
# MAGIC       THEN 'On time'
# MAGIC
# MAGIC       ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC     END AS OnTime
# MAGIC
# MAGIC FROM radar.clients_historical t1
# MAGIC LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC WHERE
# MAGIC     t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC     AND YEAR(t1.CDDExecution) = YEAR(CURRENT_DATE)
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.CDDExecution) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC     AND MONTH(t1.CDDExecution) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC
# MAGIC GROUP BY 1,2,3,7,8

# COMMAND ----------

# DBTITLE 1,pad_on_time_historical
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pad_on_time_historical AS
# MAGIC
# MAGIC -- EXCLUDE CLIENTS WITH OFFBOARDING AS THE LATEST CASE REVIEW TYPE
# MAGIC WITH offboarded_cte AS (
# MAGIC   WITH max_cases_cte AS (
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Offboarding' THEN CaseId END) AS max_offboarding_id
# MAGIC       , MAX(CASE WHEN CaseReviewType = 'Periodic Review' THEN CaseId END) AS max_periodic_id
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType IN ('Offboarding','Periodic Review')
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   , max_periodic_prework AS (
# MAGIC     -- get the latest prework date for each periodic review
# MAGIC     SELECT
# MAGIC       UniqueGcobId
# MAGIC       , MAX(Prework) AS max_prework
# MAGIC     FROM radar.cases_historical
# MAGIC     WHERE EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND CaseReviewType = 'Periodic Review'
# MAGIC     GROUP BY UniqueGcobId
# MAGIC   )
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     t1.UniqueGcobId
# MAGIC
# MAGIC   FROM radar.clients_historical t1
# MAGIC   JOIN max_cases_cte t2 ON t2.UniqueGcobId = t1.UniqueGcobId
# MAGIC     -- only keep those whose max caseid for offboarding > max caseid for periodic review
# MAGIC     AND t2.max_offboarding_id > t2.max_periodic_id
# MAGIC   LEFT JOIN max_periodic_prework t3 ON t1.UniqueGcobId = t3.UniqueGcobId
# MAGIC
# MAGIC   WHERE t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1) -- 2025-01-01
# MAGIC     AND YEAR(t1.ClientCaseInitiationStart) = YEAR(CURRENT_DATE) -- 2025
# MAGIC     AND t1.Scope = 'backToGreen'
# MAGIC     AND t1.ClientLifeCycleName = 'Client'
# MAGIC     AND MONTH(t1.ClientCaseInitiationStart) BETWEEN MONTH(make_date(YEAR(CURRENT_DATE), 1, 1)) AND MONTH(DATEADD(MONTH,-1,(SELECT MAX(edl_loaddate) FROM radar.clients_historical))) -- PID after or equal to 2025-01-01 AND less or equal to current month
# MAGIC
# MAGIC     -- only if the year(max(t3.Prework)) <> YEAR(CURRENT_DATE) --> exclude the clients that actually had a PR in the current year
# MAGIC     AND t3.max_prework IS NOT NULL
# MAGIC     AND YEAR(t3.max_prework) <> YEAR(CURRENT_DATE)
# MAGIC )
# MAGIC
# MAGIC , pad_on_time_overview_cte AS (
# MAGIC     SELECT
# MAGIC       t1.UniqueGcobId
# MAGIC       , t1.SourceClient
# MAGIC       , t1.CDDExecution AS CDDExecution_old
# MAGIC       , MAX(t2.SignOff) AS SignOff_new
# MAGIC       , MAX(t2.ReadyForAssessment) AS ReadyForAssessment_new
# MAGIC       , datediff(t1.CDDExecution, MAX(t2.ReadyForAssessment)) as DateDiff_CDDExecution_ReadyForAssessment
# MAGIC       , t3.CDDExecution AS CDDExecution_new
# MAGIC
# MAGIC       , CASE
# MAGIC           WHEN t1.UniqueGcobId IN (SELECT * FROM offboarded_cte) THEN 'Offboarding'
# MAGIC
# MAGIC           WHEN DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > t1.CDDExecution THEN 'Not on time' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN MAX(t2.ReadyForAssessment) IS NULL THEN 'Not on time' -- DOUBLE CHECK THIS, MIGHT BE OFFBOARDING CASES!!!!
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (t1.CDDExecution <> t3.CDDExecution)
# MAGIC             AND (MAX(t2.SignOff) IS NOT NULL AND MAX(t2.SignOff) < t1.CDDExecution)
# MAGIC           THEN 'On time' -- or rescheduled' -- substract 1 day otherwise cases started on same day are Not on time
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (MAX(t2.SignOff) IS NULL OR MAX(t2.SignOff) > t1.CDDExecution)
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) > MAX(t2.SignOff))
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           WHEN (DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)) < t1.CDDExecution)
# MAGIC             AND (MAX(t2.SignOff) > DATEADD(DAY, -1, MAX(t2.ReadyForAssessment)))
# MAGIC             AND (DATEADD(DAY, -1, MAX(t2.SignOff)) < t1.CDDExecution) -- handle when ready and sign off are both before cddexecution
# MAGIC           THEN 'On time'
# MAGIC
# MAGIC           ELSE 'Not on time' -- - to be verified'
# MAGIC
# MAGIC         END AS OnTime
# MAGIC
# MAGIC     FROM radar.clients_historical t1
# MAGIC     LEFT JOIN radar.cases_historical t2 ON t1.UniqueGcobId = t2.UniqueGcobId AND t2.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical) AND t2.CaseReviewType in ('Periodic Review') --, 'Offboarding')
# MAGIC     LEFT JOIN radar.clients_historical t3 ON t1.UniqueGcobId = t3.UniqueGcobId AND t3.EDL_LoadDate = (SELECT MAX(edl_loaddate) FROM radar.clients_historical)
# MAGIC
# MAGIC     WHERE
# MAGIC       t1.EDL_LoadDate = make_date(YEAR(CURRENT_DATE), 1, 1)
# MAGIC       AND YEAR(t1.CDDExecution) = YEAR(CURRENT_DATE)
# MAGIC       AND t1.Scope = 'backToGreen'
# MAGIC       AND t1.ClientLifeCycleName = 'Client'
# MAGIC     GROUP BY 1,2,3,7
# MAGIC   )
# MAGIC
# MAGIC SELECT 
# MAGIC   MONTH(CDDExecution_old) AS Month
# MAGIC   , COUNT(DISTINCT UniqueGcobId) AS CasesCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('On time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('On time or rescheduled') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesOnTimeOrRescheduledCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Not on time') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesNotOnTimeCount
# MAGIC
# MAGIC   -- , CASE 
# MAGIC   --     WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC   --       AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC   --     THEN COUNT(DISTINCT CASE 
# MAGIC   --       WHEN OnTime IN ('Not on time - to be verified') 
# MAGIC   --       THEN UniqueGcobId 
# MAGIC   --       ELSE NULL 
# MAGIC   --     END)
# MAGIC   --     ELSE NULL
# MAGIC   --   END AS CasesNotOnTimeToBeVerifiedCount
# MAGIC
# MAGIC   , CASE 
# MAGIC       WHEN MONTH(CDDExecution_old) >= MONTH(make_date(YEAR(CURRENT_DATE), 1, 1))
# MAGIC         AND MONTH(CDDExecution_old) <= MONTH(DATEADD(MONTH, -1, (SELECT MAX(edl_loaddate) FROM radar.clients_historical)))
# MAGIC       THEN COUNT(DISTINCT CASE 
# MAGIC         WHEN OnTime IN ('Offboarding') 
# MAGIC         THEN UniqueGcobId 
# MAGIC         ELSE NULL 
# MAGIC       END)
# MAGIC       ELSE NULL
# MAGIC     END AS CasesOffboardedCount
# MAGIC
# MAGIC FROM pad_on_time_overview_cte
# MAGIC GROUP BY MONTH(CDDExecution_old)
# MAGIC ORDER BY MONTH(CDDExecution_old) ASC

# COMMAND ----------

# DBTITLE 1,drop tables
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS radar.pid_on_time_overview_historical;
# MAGIC DROP TABLE IF EXISTS radar.pid_on_time_historical;
# MAGIC DROP TABLE IF EXISTS radar.pad_on_time_overview_historical;
# MAGIC DROP TABLE IF EXISTS radar.pad_on_time_historical;

# COMMAND ----------

# DBTITLE 1,store tables
spark.sql('SELECT * FROM pid_on_time_overview_historical').write.mode('overwrite').saveAsTable('radar.pid_on_time_overview_historical')
spark.sql('SELECT * FROM pid_on_time_historical').write.mode('overwrite').saveAsTable('radar.pid_on_time_historical')
spark.sql('SELECT * FROM pad_on_time_overview_historical').write.mode('overwrite').saveAsTable('radar.pad_on_time_overview_historical')
spark.sql('SELECT * FROM pad_on_time_historical').write.mode('overwrite').saveAsTable('radar.pad_on_time_historical')
