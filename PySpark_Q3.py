user_df = finalDF
user_df.createOrReplaceTempView("user")
Content_df.createOrReplaceTempView("content")
Engagement_df.createOrReplaceTempView("engagement")

# 1.
t = spark.sql("select Location,count(*) count from user group by Location")
t.createOrReplaceTempView("t1")
spark.sql("select Location from t1 where count = (select max(count)from t1) ").show()

# 2.
max_user = spark.sql("select Subscription count from user group by Subscription order by count(*) desc limit 1")
max_user.show()
min_user = spark.sql("select Subscription,count(*) count from user group by Subscription order by count(*) limit 1")
min_user.show()

# 3.
temp = spark.sql("select ShowID,avg(Rating) avg from user group by ShowID order by avg(Rating) desc ")
temp.createOrReplaceTempView("t2")
spark.sql("select ShowID from t2 where avg = (select max(avg) from t2) ").show()
# 4.
temp = spark.sql("select Location,avg(Rating) avg from user group by Location order by avg(Rating)  desc ")
temp.show()
temp.createOrReplaceTempView("t3")
spark.sql("select Location from t3 where avg = (select max(avg) from t3) ").show()

# 5.
temp = spark.sql("select ShowID,count(*) count from user group by ShowID order by count(*) desc")
temp.createOrReplaceTempView("t4")
spark.sql("select ShowID from t4 where count = (select max(count) from t4) ").show()

# 6.
user_df.agg(min(to_date("Timestamp","yyyy-MM-dd")).alias("Timestamp")).show()

# 7.
spark.sql("select ShowID,sum(Rating) TotalRating from user group by ShowID ").show()

# 8.
spark.sql("select Subscription,cast((count(*)/3) as int) count from user where age between 18 and 30 group by Subscription ").show()
