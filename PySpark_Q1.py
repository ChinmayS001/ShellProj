import pyspark
from pyspark.sql.types import *
from collections import *
import pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql.window import *
from datetime import *

UserRDD = sc.textFile("/User_Data.csv")
ContentRDD = sc.textFile("/Content_Data.csv")
EngagementRDD = sc.textFile("/Engagement_Data.csv")
h1 = UserRDD.first()
UserRDD = UserRDD.filter(lambda s: s!=h1).map(lambda s:s.split(",")).map(lambda r: (r[0],int(r[1]),r[2],r[3],r[4]))
UserRDD.take(5)
UserRDD.count()
h2 = ContentRDD.first()
ContentRDD = ContentRDD.filter(lambda s: s!=h2).map(lambda s: s.split(",")).map(lambda r: (r[0],r[1],r[2],r[3],int(r[4]),r[5]))
ContentRDD.take(5)
ContentRDD.count()
h3 = EngagementRDD.first()
EngagementRDD = EngagementRDD.filter(lambda s: s!=h3).map(lambda s: s.split(",")).map(lambda r: (r[0],r[1],r[2],r[3],int(r[4])))
EngagementRDD.take(5)
EngagementRDD.count()

#b

# removing rows who have ‘?’ in column UserID and ShowID
EngagementRDD = EngagementRDD.filter(lambda r: r[0]!='?' and r[1]!='?')
ContentRDD = ContentRDD.filter(lambda r: r[0]!='?')
UserRDD = UserRDD.filter(lambda r: r[0]!='?')
# removing dublicates
EngagementRDD = EngagementRDD.distinct()
ContentRDD = ContentRDD.distinct()
UserRDD = UserRDD.distinct()
for i in UserRDD.collect():
     print(i)
for i in ContentRDD.collect():
     print(i)
for i in EngagementRDD.collect():
     print(i)


#c


Content_df = ContentRDD.toDF(['ShowID','Genre','Actors','Director','Release_Year','Synopsis'])
Content_df.createOrReplaceTempView("temp2")
temp = spark.sql(""" select ShowID,Genre,trim(both'"' from Actors) as Actors,Director,Release_Year,Synopsis from temp2 """)
temp.coalesce(1).write.mode("overwrite").parquet("file:///home/chinmay/CapStoneProject/Content_Data")
Engagement_df = EngagementRDD.toDF(['UserID','ShowID','PlaybackStarted','PlaybackStopped','CompletionPercent'])
Engagement_df.coalesce(1).write.mode("overwrite").parquet("file:///home/chinmay/CapStoneProject/Engagement_Data")
User_df = UserRDD.toDF(['UserID','Age','Location','Subscription','WatchHistory'])
User_df.createOrReplaceTempView("temp1")
temp_df = spark.sql("""select UserID,Age,trim(both'"' from Location) as Location,trim(both'"' from Subscription) as Subscription, WatchHistory from temp1 """)
temp_df.coalesce(1).write.mode("overwrite").parquet("file:///home/chinmay/CapStoneProject/User_Data")

