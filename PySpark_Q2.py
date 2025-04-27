# a :
Content_df = spark.read.parquet("file:///home/chinmay/CapStoneProject/Content_Data")
Engagement_df = spark.read.parquet("file:///home/chinmay/CapStoneProject/Engagement_Data")
User_df = spark.read.parquet("file:///home/chinmay/CapStoneProject/User_Data")


# b :
tempDF = User_df.withColumn('watch',split('WatchHistory','\\|'))
tempDF = tempDF.withColumn('xyz',explode('watch'))
split_cols = split('xyz',';')
tempDF = tempDF.withColumn('ShowID',trim(split_cols.getItem(0))).withColumn('Timestamp',trim(split_cols.getItem(1))).withColumn('Rating',trim(split_cols.getItem(2)))                
finalDF = tempDF.select('UserID','Age','Location','Subscription','ShowID','Timestamp','Rating')
finalDF.show(truncate=False)

# c

finalDF.coalesce(1).write.mode("overwrite").parquet("file:///home/chinmay/CapStoneProject/split_User_Data")