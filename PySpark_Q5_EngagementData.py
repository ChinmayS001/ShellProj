# 16.
spark.sql("select ShowID from engagement where CompletionPercent = (select max(CompletionPercent) from engagement) ").show()

# 17.
Engagement_df.select("UserID",datediff(to_date('PlaybackStopped','yyyy-MM-dd'),to_date('PlaybackStarted','yyyy-MM-dd')).alias("TimeDurationInDays")).show()
#Not Working

# 18.
temp = Engagement_df.select("UserID",datediff(to_date('PlaybackStopped','yyyy-MM-dd'),to_date('PlaybackStarted','yyyy-MM-dd')).alias("TimeDurationInDays"))
temp.createOrReplaceTempView("t7")
spark.sql("select UserID from t7 where TimeDurationInDays = (select max(TimeDurationInDays) from t7)").show()
#Not Working

# 19.
spark.sql("select ShowID from engagement group by ShowID having avg(CompletionPercent)>60 ").show()

# 20.
spark.sql("select ShowID from engagement where CompletionPercent = (select min(CompletionPercent) from engagement) ").show()
