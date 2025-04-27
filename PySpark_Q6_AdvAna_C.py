# 1
spark.sql("select ShowID, cast(date_format(Timestamp,'yyyy') as varchar(4)) Year, count(*) count from t1 where cast(date_format(Timestamp,'yyyy') as varchar(4))='2017' and showID='M8' group by ShowID,Year").show()

# 2

temp = Engagement_df.select("UserID",datediff(to_date('PlaybackStopped','yyyy-MM-dd'),to_date('PlaybackStarted','yyyy-MM-dd')).alias("TimeDurationInDays"))
temp.createOrReplaceTempView("t7")
spark.sql("select UserID from t7 where TimeDurationInDays = (select max(TimeDurationInDays) from t7)").show()
spark.sql("").show()



from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()


from pyspark.sql.types import StringType
def DateClean(col):
    arr = col.split('/')
    arr[0] = '0'+arr[0] if len(arr[0]) == 1 else arr[0]
    arr[1] = '0'+arr[1] if len(arr[1]) == 1 else arr[1]
    return '-'.join(arr)

# df2 = spark.sql("select DateClean_udf(PlaybackStarted) co from dataa")
spark.udf.register("CleanDatee", DateClean, StringType())


dateFormatted = spark.sql("with newTB as (select * from Engagement where PlaybackStarted like '%/%/%' and "
"PlaybackStopped like '%/%/%') select UserID,ShowID,cleanDatee(PlaybackStarted) PStart,cleanDatee(PlaybackStopped)"
"PStop,CompletionPercent from newTB")
dateFormatted.createOrReplaceTempView("EngagementFormatted");

 TempDF = spark.sql("select UserID,ShowID,datediff(to_date('PStop','yyyy-MM-dd'),to_date('PStart','yyyy-MM-dd')) PlaybackTime,CompletionPercent from EngagementFormatted");
# temp = Engagement_df.select("UserID",datediff(to_date('PlaybackStopped','yyyy-MM-dd'),to_date('PlaybackStarted','yyyy-MM-dd')).alias("TimeDurationInDays"))
# spark.stop()