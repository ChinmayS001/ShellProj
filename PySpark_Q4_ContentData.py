# 9. 
Content_df.select("Genre").distinct().show()
q_9 = Content_df.select("Genre").distinct()
q_9.write.parquet("file:///home/chinmay/CapStoneProject/Spark_SQl")

# 10.
t = spark.sql("select Director,count(*) count from content group by Director ")
t.createOrReplaceTempView("t5")
spark.sql("select Director from t5 where count in (select max(count) from t5) ").show()
q_10 = spark.sql("select Director from t5 where count in (select max(count) from t5) ")

# 11.
spark.sql("select Release_Year,count(*) from content group by Release_Year ").show()
q_11 = spark.sql("select Release_Year,count(*) from content group by Release_Year ")
q_11.show()

# 12.
temp_df = Content_df.withColumn("Actor",split("Actors","\\|"))
temp_df = temp_df.select("ShowID","Genre",explode("Actor").alias("ACTOR"),"Director","Release_Year","Synopsis")
temp_df = temp_df.withColumnRenamed("ACTOR","Actor")
temp_df.show()

# 13.

# 14.
spark.sql("select count(*) count from content where Genre='Comedy' ").show()
q_14 = spark.sql("select count(*) count from content where Genre='Comedy' ")
q_14.show()

# 15.
t = spark.sql("select Director,count(*) count from content group by Director ")
t.createOrReplaceTempView("t6")
spark.sql("select Director from t6 where count in (select max(count) from t6) ").show()
spark.sql("select Director from t6 where count in (select min(count) from t6) ").show()
