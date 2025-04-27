
spark.sql("select ShowID, cout(*) NoOfUsers from Engagement where CompletionPercent = 100 group by ShowID")


spark.sql("select ShowID, avg(CompletionPercent) AvgCompletion from Engagement")

spark.sql("select ")



