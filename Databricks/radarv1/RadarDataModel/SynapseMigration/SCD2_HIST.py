# Part 1. Preparation: get filepaths in order of all files to loop scd2 over.

# Part 2. get the first table and create delta table
FirstFile_Date = filepaths_list[0]
FirstFile_Date =FirstFile_Date[-10:]#taking only date part
FirstFile_Date

Firstfile_df=spark.read.load(f'{path_loop}{FirstFile_Date}/*.parquet', format="parquet")

for colum in Firstfile_df.columns:
    starttable = Firstfile_df.withColumnRenamed(colum, colum.replace(' ', '_'))

hash_column=[]
for item in starttable.columns:
    if item not in ('EDL_LOAD_DTS'):
        #print(item)
        hash_column.append(item)

# select columns to hash over
#HashCols = starttable.columns #TODO: should be removing columns that are specific for that date (like EDL_LOAD_DTS, EDL_LOAD_DTS_UTC, EDL_ACT_DTS, EDL_ACT_DTS_UTC)
HashCols=hash_column
starttable = starttable.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256)))
starttable = starttable.withColumn("StartDate", to_date(F.lit(FirstFile_Date), "yyyy/MM/dd"))
starttable = starttable.withColumn("EndDate",to_date(F.lit("9999-12-31"),"yyyy-MM-dd"))
starttable = starttable.withColumn("Active_status",F.lit("Y"))   


# Part 3. Creating Delta Table for 1st File
# TODO: this has hardcoded GIC.
starttable.write.mode("overwrite").format("delta").saveAsTable(f'radar_scd2.GIC_{Source_FileName}')


# Part 4. now we will start looping over other files and apply MERGE INTO statement.
## Looping over files
for file_date in filepaths_list[1:-1]:

    file_date = file_date[-10:]#taking only date part
    fileStartdate = datetime.strptime(file_date, '%Y/%m/%d').date()
    df=spark.read.load(f'{path_loop}{file_date}/*.parquet', format="parquet")

    hash_column=[]
    for item in df.columns:
        if item not in ('EDL_LOAD_DTS'):
        #print(item)
            hash_column.append(item)   
    HashCols=hash_column

    for column in hash_column:
        df = df.withColumnRenamed(column, column.replace(' ', '_'))
    # select columns to hash over 

    #adding new columns
    df = df.withColumn("Hash", lit(sha2(concat_ws("~", *HashCols), 256)))
    df = df.withColumn("StartDate", to_date(F.lit(file_date), "yyyy/MM/dd"))
    df = df.withColumn("EndDate",to_date(F.lit("9999-12-31"),"yyyy-MM-dd"))
    df = df.withColumn("Active_status",F.lit("Y"))
    Source_FileName_table=Source_FileName.lower()#making it as lower case since while creating tablename is going as lower case

    DeltaTarget = DeltaTable.forPath(spark, path = f'abfss://adlsasafecreportprd@adlsasafecreportprd.dfs.core.windows.net/synapse/workspaces/asafecreportprd/warehouse/radar_scd2.db/gic_{Source_FileName_table}')
    #Now apply MERGE INTO logic.
    df = df.drop_duplicates(subset = ['Hash'])

    #3 Options to consider, only select the active rows in the target.
    # Hash is active record in source and target: do nothing. possibly update field 'latest seen'
    # hash exists in target but not in source: update target row to set expired columns and update boolean for the active record                 
    # hash exists in source but not in target: insert new row
    print(fileStartdate)
    #spark.sql('select * from radar_scd2.kn1_adkyc_riscos')

    MergeActivity = DeltaTarget.alias('dt').merge(df.alias('df'), 'dt.Hash = df.Hash and dt.Active_status = "Y" ') \
                        .whenNotMatchedBySourceUpdate(condition = 'dt.Active_status = "Y" ', set = {"dt.EndDate": f'"{fileStartdate}"', "dt.Active_status": "'N'" }) \
                        .whenNotMatchedInsertAll()

                        
    #.whenMatchedUpdate(set = {"Active_status": "df.Active_status"})

    MergeActivity.execute()

    print('executed', file_date, df.count())

    #optional: .withSchemaEvolution()


   