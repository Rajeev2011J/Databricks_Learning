# Databricks notebook source
import os

app_reg_app_id = os.environ['APP_REG_APP_ID']
ReadStorage = os.environ['GDP_STORAGE_NAME']
TenantId = os.environ['TENANT_ID']

service_credential = dbutils.secrets.get(scope="connectedsecrets", key = f"appreg-{app_reg_app_id}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+ReadStorage+".dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type."+ReadStorage+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
spark.conf.set("fs.azure.account.oauth2.client.id."+ReadStorage+".dfs.core.windows.net", ""+app_reg_app_id+"") 
spark.conf.set("fs.azure.account.oauth2.client.secret."+ReadStorage+".dfs.core.windows.net", service_credential) 
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+ReadStorage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Load Siebel Data
from pyspark.sql import SparkSession

# Load and create temp views of all gdp_tables below
siebel_tables = {
    'cdf_ggm_rel_x_ar_hist': '2',
    'cdf_ggm_org_hist': '1',
    'cdf_ggm_ar_hist': '1'
}

for item, version in siebel_tables.items():
    # Get the most recent file available in gdp
    path = f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('edl_partition_date=')[1][:8] for file in files if 'edl_partition_date=' in file.path)

    spark.read.format('delta').load(f'abfss://siebel-idaa-cdf@edlcorestdeuprod0001.dfs.core.windows.net/{item}/{version}/data').createOrReplaceTempView('siebel_' + item)


# COMMAND ----------

# DBTITLE 1,Load GCDS Data
from pyspark.sql import SparkSession
# load and create temp views of all gdp_tables below
gcds_tables = [
      'client_KeyStoreKey'
    , 'client_Client'
    , 'client_PartyRole'
]

for item in gcds_tables:
    # get the most recent file available in gdp
    path = f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/'
    files = dbutils.fs.ls(path)
    load_date = max(file.path.split('LOAD_DTS=')[1][:8] for file in files if 'LOAD_DTS=' in file.path)

    spark.read.parquet(f'abfss://gcds@edlcorestdeuprod0001.dfs.core.windows.net/{item}/4602/data/LOAD_DTS={load_date}*/*.parquet').createOrReplaceTempView('gcds_'+ item)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   t3.rel_id
# MAGIC   , t3.ORG_LGL_NM AS siebelClientName
# MAGIC   , t3.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName
# MAGIC   , t6.cmrcl_pd_tp_ctlg_nm AS siebelProductName
# MAGIC   , t6.ar_st_ggm_dsc AS siebelProductLifeCycleName
# MAGIC   , t6.ar_del_f AS siebelEDLFlag
# MAGIC   , t3.Edl_valid_to_dts
# MAGIC   , t6.Edl_valid_to_dts
# MAGIC
# MAGIC from siebel_cdf_ggm_org_hist t3
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_ar_hist t6 ON t6.rel_id = t3.rel_id
# MAGIC
# MAGIC where t3.rel_id in (
# MAGIC   '000000105571665'
# MAGIC   , '000000104704489'
# MAGIC   , '000000113679634'
# MAGIC   , '000000113512625'
# MAGIC   , '000000115333698'
# MAGIC   , '000000119899306'
# MAGIC   , '000000118080828'
# MAGIC   , '000000115719847'
# MAGIC   , '000000116573077'
# MAGIC   , '000000118546750'
# MAGIC   , '000000116559904'
# MAGIC   , '000000118067579'
# MAGIC   , '000000102854546'
# MAGIC   , '000000102908141'
# MAGIC   , '000000117574368'
# MAGIC   , '000000117682403'
# MAGIC   , '000000116632995'
# MAGIC   , '000000103612011'
# MAGIC   , '000000113784014'
# MAGIC   , '000000119975375'
# MAGIC   , '000000118240301'
# MAGIC   , '000000116969173'
# MAGIC   , '000000116457849'
# MAGIC   , '000000117191323'
# MAGIC   , '000000116969629'
# MAGIC   , '000000106610043'
# MAGIC   , '000000106828151'
# MAGIC   , '000000118528761'
# MAGIC   , '000000116970772'
# MAGIC   , '000000118881332'
# MAGIC   , '000000114526331'
# MAGIC   , '000000119252130'
# MAGIC   , '000000115507785'
# MAGIC   , '000000116715113'
# MAGIC   , '000000116350298'
# MAGIC   , '000000117804503'
# MAGIC   , '000000117333045'
# MAGIC   , '000000117347476'
# MAGIC   , '000000117582487'
# MAGIC   , '000000117915788'
# MAGIC   , '000000118488350'
# MAGIC   , '000000115607751'
# MAGIC   , '000000118502661'
# MAGIC   , '000000116086487'
# MAGIC   , '000000118625435'
# MAGIC   , '000000116677200'
# MAGIC   , '000000116577575'
# MAGIC   , '000000119899765'
# MAGIC   , '000000119043600'
# MAGIC   , '000000117928331'
# MAGIC   , '000000116841631'
# MAGIC   , '000000116859193'
# MAGIC   , '000000116683093'
# MAGIC   , '000000102939164'
# MAGIC   , '000000102917686'
# MAGIC   , '000000117953553'
# MAGIC   , '000000116761142'
# MAGIC   , '000000011763036'
# MAGIC   , '000000117953451'
# MAGIC   , '000000011761173'
# MAGIC   , '000000102952309'
# MAGIC   , '000000011763758'
# MAGIC   , '000000011763450'
# MAGIC   , '000000011763460'
# MAGIC   , '000000011763473'
# MAGIC   , '000000011763476'
# MAGIC   , '000000011763457'
# MAGIC   , '000000011763445'
# MAGIC   , '000000011763480'
# MAGIC   , '000000011763477'
# MAGIC   , '000000011763475'
# MAGIC   , '000000011763471'
# MAGIC   , '000000011763469'
# MAGIC   , '000000011763464'
# MAGIC   , '000000011763461'
# MAGIC   , '000000011763447'
# MAGIC   , '000000011763446'
# MAGIC   , '000000011763444'
# MAGIC   , '000000011763443'
# MAGIC   , '000000011763584'
# MAGIC   , '000000011763536'
# MAGIC   , '000000011763533'
# MAGIC   , '000000011763524'
# MAGIC   , '000000103871973'
# MAGIC   , '000000011762553'
# MAGIC   , '000000117953489'
# MAGIC   , '000000105894298'
# MAGIC   , '000000011761891'
# MAGIC   , '000000117193176'
# MAGIC   , '000000011763453'
# MAGIC   , '000000118087342'
# MAGIC   , '000000102885848'
# MAGIC   , '000000112881868'
# MAGIC   , '000000102834525'
# MAGIC   , '000000011763494'
# MAGIC   , '000000011763402'
# MAGIC   , '000000011763498'
# MAGIC   , '000000102794164'
# MAGIC   , '000000011763467'
# MAGIC   , '000000117180291'
# MAGIC   , '000000120008651'
# MAGIC   , '000000118380829'
# MAGIC   , '000000116575308'
# MAGIC   , '000000116828859'
# MAGIC   , '000000102857774'
# MAGIC   , '000000111418366'
# MAGIC   , '000000102971563'
# MAGIC   , '000000118791878'
# MAGIC   , '000000118791917'
# MAGIC   , '000000116700606'
# MAGIC   , '000000102032812'
# MAGIC   , '000000011762005'
# MAGIC   , '000000011763022'
# MAGIC   , '000000102841500'
# MAGIC   , '000000115363409'
# MAGIC   , '000000102886790'
# MAGIC   , '000000011762564'
# MAGIC   , '000000106779477'
# MAGIC   , '000000101929657'
# MAGIC   , '000000117202858'
# MAGIC   , '000000011761562'
# MAGIC   , '000000118547978'
# MAGIC   , '000000118547861'
# MAGIC   , '000000105452752'
# MAGIC   , '000000112535159'
# MAGIC   , '000000116617320'
# MAGIC   , '000000115599025'
# MAGIC   , '000000117602345'
# MAGIC   , '000000102840274'
# MAGIC   , '000000105189335'
# MAGIC   , '000000102840315'
# MAGIC   , '000000102833683'
# MAGIC   , '000000116587921'
# MAGIC   , '000000119454350'
# MAGIC   , '000000123275731'
# MAGIC   , '000000110750118'
# MAGIC   , '000000118411032'
# MAGIC   , '000000118518010'
# MAGIC   , '000000114849366'
# MAGIC   , '000000119824167'
# MAGIC   , '000000118387019'
# MAGIC   , '000000111819038'
# MAGIC   , '000000116683161'
# MAGIC   , '000000111160911'
# MAGIC   , '000000114990465'
# MAGIC   , '000000117343281'
# MAGIC   , '000000120523758'
# MAGIC   , '000000118454841'
# MAGIC   , '000000119609546'
# MAGIC   , '000000117534427'
# MAGIC   , '000000116518843'
# MAGIC   , '000000116514066'
# MAGIC   , '000000119353469'
# MAGIC   , '000000116514000'
# MAGIC   , '000000120505481'
# MAGIC   , '000000116783030'
# MAGIC   , '000000106252587'
# MAGIC   , '000000106286252'
# MAGIC   , '000000116359390'
# MAGIC   , '000000117164407'
# MAGIC   , '000000115782948'
# MAGIC   , '000000106357926'
# MAGIC   , '000000116683864'
# MAGIC   , '000000112317802'
# MAGIC   , '000000110907613'
# MAGIC   , '000000102918546'
# MAGIC   , '000000111607257'
# MAGIC   , '000000105978187'
# MAGIC   , '000000116681547'
# MAGIC   , '000000118416617'
# MAGIC   , '000000116066044'
# MAGIC   , '000000116064766'
# MAGIC   , '000000106272515'
# MAGIC   , '000000116249824'
# MAGIC   , '000000112884369'
# MAGIC   , '000000106032413'
# MAGIC   , '000000114606287'
# MAGIC   , '000000106220698'
# MAGIC   , '000000106220489'
# MAGIC   , '000000106258806'
# MAGIC   , '000000106280329'
# MAGIC   , '000000106270662'
# MAGIC   , '000000106415750'
# MAGIC   , '000000106271763'
# MAGIC   , '000000117483067'
# MAGIC   , '000000115118677'
# MAGIC   , '000000106203465'
# MAGIC   , '000000119612725'
# MAGIC   , '000000115942007'
# MAGIC   , '000000115470840'
# MAGIC   , '000000120317496'
# MAGIC   , '000000120317533'
# MAGIC   , '000000116093838'
# MAGIC   , '000000114407203'
# MAGIC   , '000000105878535'
# MAGIC   , '000000105878644'
# MAGIC   , '000000105875169'
# MAGIC   , '000000103498681'
# MAGIC   , '000000118446662'
# MAGIC   , '000000118774073'
# MAGIC   , '000000116064753'
# MAGIC   , '000000101516410'
# MAGIC   , '000000117581680'
# MAGIC   , '000000116800996'
# MAGIC   , '000000116869828'
# MAGIC   , '000000116682238'
# MAGIC   , '000000117190996'
# MAGIC   , '000000116684454'
# MAGIC   , '000000113767721'
# MAGIC   , '000000105728899'
# MAGIC   , '000000102858561'
# MAGIC   , '000000116948307'
# MAGIC   , '000000105283566'
# MAGIC   , '000000116426774'
# MAGIC   , '000000106347567'
# MAGIC   , '000000116680784'
# MAGIC
# MAGIC )
# MAGIC
# MAGIC AND t3.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.ar_del_f = 'N'
# MAGIC AND t6.ar_st_ggm_dsc = 'Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   t3.rel_id
# MAGIC   , t3.ORG_LGL_NM AS siebelClientName
# MAGIC   , t3.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName
# MAGIC   , t6.cmrcl_pd_tp_ctlg_nm AS siebelProductName
# MAGIC   , t6.ar_st_ggm_dsc AS siebelProductLifeCycleName
# MAGIC   , t6.ar_del_f AS siebelEDLFlag
# MAGIC   , t3.Edl_valid_to_dts
# MAGIC   , t6.Edl_valid_to_dts
# MAGIC
# MAGIC from siebel_cdf_ggm_org_hist t3
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_ar_hist t6 ON t6.rel_id = t3.rel_id
# MAGIC
# MAGIC where t3.rel_id in (
# MAGIC   '000000118591720'
# MAGIC   , '000000106256595'
# MAGIC   , '000000116865456'
# MAGIC   , '000000119473313'
# MAGIC   , '000000102893763'
# MAGIC )
# MAGIC
# MAGIC AND t3.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.ar_del_f = 'N'
# MAGIC AND t6.ar_st_ggm_dsc = 'Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   t3.rel_id
# MAGIC   , t3.ORG_LGL_NM AS siebelClientName
# MAGIC   , t3.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName
# MAGIC   , t6.cmrcl_pd_tp_ctlg_nm AS siebelProductName
# MAGIC   , t6.ar_st_ggm_dsc AS siebelProductLifeCycleName
# MAGIC   , t6.ar_del_f AS siebelEDLFlag
# MAGIC   , t3.Edl_valid_to_dts
# MAGIC   , t6.Edl_valid_to_dts
# MAGIC
# MAGIC from siebel_cdf_ggm_org_hist t3
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_ar_hist t6 ON t6.rel_id = t3.rel_id
# MAGIC
# MAGIC where t3.rel_id in (
# MAGIC   '000000104015693'
# MAGIC   , '000000120319110'
# MAGIC )
# MAGIC
# MAGIC AND t3.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.Edl_valid_to_dts like '9999-12-31%'
# MAGIC AND t6.ar_del_f = 'N'
# MAGIC AND t6.ar_st_ggm_dsc = 'Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t2.gcid
# MAGIC     , t5.Full_legal_name AS gcdsClientName
# MAGIC     , t4.Life_cycle_status AS gcdsClientLifeCycleName
# MAGIC     , t3.rel_id
# MAGIC     , t3.ORG_LGL_NM AS siebelClientName
# MAGIC     , t3.rel_st_tp_ggm_dsc AS siebelClientLifeCycleName
# MAGIC     , t6.cmrcl_pd_tp_ctlg_nm AS siebelProductName
# MAGIC     , t6.ar_st_ggm_dsc AS siebelProductLifeCycleName
# MAGIC     , t6.ar_del_f AS siebelEDLFlag
# MAGIC     , t3.Edl_valid_to_dts
# MAGIC     , t6.Edl_valid_to_dts
# MAGIC
# MAGIC FROM gcds_client_KeyStoreKey t2
# MAGIC LEFT JOIN siebel_cdf_ggm_org_hist t3 ON t2.KeyStore_value = t3.rel_id
# MAGIC LEFT JOIN gcds_client_PartyRole t4 ON t2.gcid = t4.gcid
# MAGIC LEFT JOIN gcds_client_client t5 ON t2.gcid = t5.gcid
# MAGIC LEFT JOIN siebel_cdf_ggm_rel_x_ar_hist t6 ON t6.rel_id = t3.rel_id
# MAGIC WHERE
# MAGIC     t2.KeyStore_type IN ('SBWRR')
# MAGIC     -- AND t3.bnk_code IN ('3000', '3400')
# MAGIC     -- AND t4.party_role = 'Customer'
# MAGIC     -- AND t3.Edl_valid_to_dts like '9999-12-31%'
# MAGIC     -- AND t6.Edl_valid_to_dts like '9999-12-31%'
# MAGIC     -- AND t6.ar_del_f = 'N'
# MAGIC     -- AND t6.ar_st_ggm_dsc = 'Active'
# MAGIC     -- AND t3.ORG_LGL_NM = t1.FullLegalName
# MAGIC     AND t2.gcid in 
# MAGIC     (
# MAGIC   47392
# MAGIC   , 1151898
# MAGIC   , 80397
# MAGIC   , 767852
# MAGIC   , 762780
# MAGIC   , 1612
# MAGIC   , 445
# MAGIC   , 824681
# MAGIC   , 1052771
# MAGIC   , 864349
# MAGIC   , 46921
# MAGIC   , 1258898
# MAGIC   , 1259654
# MAGIC   , 864345
# MAGIC   , 715189
# MAGIC   , 918010
# MAGIC   , 864290
# MAGIC   , 16089
# MAGIC   , 16470
# MAGIC   , 1261613
# MAGIC   , 864352
# MAGIC   , 39411
# MAGIC   , 50773
# MAGIC   , 127030
# MAGIC   , 655606
# MAGIC   , 1210707
# MAGIC   , 927251
# MAGIC   , 931473
# MAGIC   , 1110113
# MAGIC   , 1238801
# MAGIC   , 78990
# MAGIC   , 1277016
# MAGIC   , 483610
# MAGIC   , 1303509
# MAGIC   , 770256
# MAGIC   , 2093671
# MAGIC   , 2017334
# MAGIC   , 1240517
# MAGIC   , 284813
# MAGIC   , 67158
# MAGIC   , 521658
# MAGIC   , 1311098
# MAGIC   , 1311097
# MAGIC   , 2367
# MAGIC   , 7017
# MAGIC   , 49525
# MAGIC   , 1391
# MAGIC   , 919232
# MAGIC   , 26997
# MAGIC   , 48736
# MAGIC   , 7114
# MAGIC   , 8590
# MAGIC   , 15482
# MAGIC   , 9422
# MAGIC   , 771630
# MAGIC   , 1305644
# MAGIC   , 29770
# MAGIC   , 1289611
# MAGIC   , 1254779
# MAGIC   , 23103
# MAGIC   , 27387
# MAGIC   , 2443974
# MAGIC   , 2008319
# MAGIC   , 844013
# MAGIC   , 620278
# MAGIC   , 914895
# MAGIC   , 80348
# MAGIC   , 8995
# MAGIC   , 49783
# MAGIC   , 26023
# MAGIC   , 20597
# MAGIC   , 53102
# MAGIC   , 22559
# MAGIC   , 15462
# MAGIC   , 2711044
# MAGIC   , 2250770
# MAGIC   , 835630
# MAGIC   , 2348713
# MAGIC   , 28579
# MAGIC   , 8247
# MAGIC   , 40867
# MAGIC   , 8925
# MAGIC   , 8631
# MAGIC   , 8914
# MAGIC   , 8855
# MAGIC   , 8863
# MAGIC   , 9023
# MAGIC   , 8850
# MAGIC   , 1096935
# MAGIC   , 48150
# MAGIC   , 8667
# MAGIC   , 2082555
# MAGIC   , 397058
# MAGIC   , 50696
# MAGIC   , 1251670
# MAGIC   , 1108830
# MAGIC   , 8240
# MAGIC   , 8225
# MAGIC   , 8200
# MAGIC   , 2349672
# MAGIC   , 2857327
# MAGIC   , 2857344
# MAGIC   , 2358951
# MAGIC   , 1979
# MAGIC   , 1111523
# MAGIC   , 113378
# MAGIC   , 856659
# MAGIC   , 903554
# MAGIC   , 67185
# MAGIC   , 31204
# MAGIC   , 2537205
# MAGIC   , 13415
# MAGIC   , 902300
# MAGIC   , 9025
# MAGIC   , 700964
# MAGIC   , 8909
# MAGIC   , 17848
# MAGIC   , 2216
# MAGIC   , 38554
# MAGIC   , 2537145
# MAGIC   , 284783
# MAGIC   , 8846
# MAGIC   , 8967
# MAGIC   , 8947
# MAGIC   , 1255478
# MAGIC   , 744058
# MAGIC   , 781071
# MAGIC   , 783341
# MAGIC   , 58719
# MAGIC   , 1256414
# MAGIC   , 1125540
# MAGIC   , 1231191
# MAGIC   , 931539
# MAGIC   , 46311
# MAGIC   , 4558
# MAGIC   , 42550
# MAGIC   , 917500
# MAGIC   , 20339
# MAGIC   , 18240
# MAGIC   , 52002
# MAGIC   , 7363
# MAGIC   , 1290756
# MAGIC   , 40709
# MAGIC   , 386
# MAGIC   , 606
# MAGIC   , 612
# MAGIC   , 769
# MAGIC   , 674
# MAGIC   , 13460
# MAGIC   , 825026
# MAGIC   , 9180
# MAGIC   , 21451
# MAGIC   , 55264
# MAGIC   , 850021
# MAGIC   , 768923
# MAGIC   , 2249442
# MAGIC   , 20723
# MAGIC   , 797
# MAGIC   , 1028
# MAGIC   , 817
# MAGIC   , 804
# MAGIC   , 809
# MAGIC   , 15153
# MAGIC   , 28626
# MAGIC   , 3312
# MAGIC   , 1254803
# MAGIC   , 826
# MAGIC   , 918015
# MAGIC   , 686
# MAGIC   , 7404
# MAGIC   , 1243509
# MAGIC   , 749
# MAGIC   , 981
# MAGIC   , 899
# MAGIC   , 836
# MAGIC   , 808
# MAGIC   , 827
# MAGIC   , 811
# MAGIC   , 761
# MAGIC   , 824
# MAGIC   , 760
# MAGIC   , 802
# MAGIC   , 756
# MAGIC   , 813
# MAGIC   , 728
# MAGIC   , 877
# MAGIC   , 838
# MAGIC   , 777
# MAGIC   , 814
# MAGIC   , 820
# MAGIC   , 810
# MAGIC   , 861
# MAGIC   , 752
# MAGIC   , 779
# MAGIC   , 919
# MAGIC   , 4080
# MAGIC   , 678
# MAGIC   , 1243510
# MAGIC   , 699
# MAGIC   , 840366
# MAGIC   , 1243515
# MAGIC   , 14314
# MAGIC   , 1516
# MAGIC   , 21969
# MAGIC   , 295198
# MAGIC   , 1263312
# MAGIC   , 2032911
# MAGIC   , 2003512
# MAGIC   , 3990
# MAGIC   , 1254894
# MAGIC   , 1290799
# MAGIC   , 1254905
# MAGIC   , 30673
# MAGIC   , 31427
# MAGIC   , 4996
# MAGIC   , 7775
# MAGIC     )
