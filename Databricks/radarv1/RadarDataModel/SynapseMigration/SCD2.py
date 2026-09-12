for column in df.columns:
    df = df.withColumnRenamed(column, column.replace(' ', '_'))
    hash_column=[]
    for item in df.columns:
        if item not in ('EDL_LOAD_DTS'):
        #print(item)
            hash_column.append(item)   
    HashCols=hash_column        
        #print(df)
    df = df.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256)))
    df = df.withColumn("StartDate", to_date(F.lit(file_date), "yyyyMMdd"))
    df = df.withColumn("EndDate",to_date(F.lit("9999-12-31"),"yyyy-MM-dd"))
    df = df.withColumn("Active_status",F.lit("Y"))
    #Source_FileName_table=Source_FileName.lower()
    DeltaTarget = DeltaTable.forPath(spark, path = f'abfss://adlsasafecreportprd@adlsasafecreportprd.dfs.core.windows.net/synapse/workspaces/asafecreportprd/warehouse/radar_scd2.db/account_all')
    #Now apply MERGE INTO logic.
    df = df.drop_duplicates(subset = ['Hash'])
    #print(fileStartdate)
    MergeActivity = DeltaTarget.alias('dt').merge(df.alias('df'), 'dt.Hash = df.Hash and dt.Active_status = "Y" ') \
                        .whenNotMatchedBySourceUpdate(condition = 'dt.Active_status = "Y" ', set = {"dt.EndDate": f'"{fileStartdate}"', "dt.Active_status": "'N'" }) \
                        .whenNotMatchedInsertAll()
    MergeActivity.execute()

    #print('executed', file_date, df.count()) 


    