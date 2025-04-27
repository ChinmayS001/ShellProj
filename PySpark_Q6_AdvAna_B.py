# 1.
temp = combine.where("Age between 18 and 30")
temp = temp.groupBy("Genre").count()
temp.createOrReplaceTempView("a1")
spark.sql("select Genre from a1 where count = (select max(count) from a1) ").show()

# 2.

