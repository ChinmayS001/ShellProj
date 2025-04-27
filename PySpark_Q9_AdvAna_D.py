
spark.sql("select ShowID, cout(*) NoOfUsers from Engagement where CompletionPercent = 100 group by ShowID").show()

spark.sql("select ShowID, avg(CompletionPercent) AvgCompletion from Engagement").show()

spark.sql("select UserID, avg(PlaybackTime) avgPlaybackTime,avg(CompletionPercent) CompletionPercent from EngagementFormatted group by UserID").show()



