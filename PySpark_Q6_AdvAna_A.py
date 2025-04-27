# 1.
user_df = spark.read.parquet("file:///home/aryan.verma02/split_User_Data")
# 2.
combine = user_df.join(Content_df,on=["ShowID"])
combine.createOrReplaceTempView("combined")
# 3.

# 4.

# 5.
