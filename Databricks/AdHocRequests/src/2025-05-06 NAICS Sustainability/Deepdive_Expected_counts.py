# Databricks notebook source
## Asia: about 450 expected.



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FEC_ESG.rdm_client
# MAGIC WHere Region_Derived = 'Asia'
# MAGIC AND LifeCycleStatus IN ('Client', 'Active')
# MAGIC AND WR_OR_RETAIL = 'W&R'
# MAGIC AND GlobalClientOwnerLocation not in ('Rabobank Hong Kong', 'Rabobank Singapore', 'Rabobank China') 
# MAGIC AND Party_type = 'Legal Entity'

# COMMAND ----------

schema(FEC_ESG)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select PartyIdentifier
# MAGIC , Party_type
# MAGIC ,  GlobalClientOwnerLocation
# MAGIC ,  BusinessLineName
# MAGIC , `W&RORRetail` AS WR_OR_RETAIL
# MAGIC ,'empty' AS GlobalClientOwnerRegion
# MAGIC , CASE  
# MAGIC         WHEN FIHubIndicator = true THEN 'FI'
# MAGIC         WHEN PartyIdentifier like '%LE_RF%' THEN 'Rabo Foundation'
# MAGIC         WHEN BusinessLineName = 'RANZ Country Banking' THEN 'RANZ Country Banking'
# MAGIC         WHEN BusinessLineName = 'RANZ Rabo Online Savings (ROS)' THEN 'RANZ Rabo Online Savings'
# MAGIC         ELSE COALESCE(t2.Region)--, t1.GlobalClientOwnerRegion) 
# MAGIC         END AS Region_Derived
# MAGIC , RefreshDate
# MAGIC , LifeCycleStatus
# MAGIC from hive_metastore.radar.RDM_Party as t1
# MAGIC LEFT JOIN locationToRegionMapping as t2 on t1.GlobalClientOwnerLocation = t2.Location

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FEC_ESG.RDM_Client
# MAGIC where 1=1
# MAGIC --AND region_derived = 'Asia'
# MAGIC AND LifeCycleStatus = 'ACTIVE'
# MAGIC

# COMMAND ----------


