from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# start spark master with: /data/spark/sbin/start-master.sh
# copy the spark_log_name from the terminal
# run cat spark_log_name
# identify and copy spark_urkl in log: spark_url = spark://c2fa894943e8462af20a0b3e85ec7df38a90ebf4-5dfc58bd9-dq5hx:7077
# start spark worker with /data/spark/sbin/./start-slave.sh spark_url
# spark worker logs are in opt/spark-2.3.4-bin-hadoop2.7/logs/spark--org.apache.spark.deploy.worker.Worker-1-c2fa894943e8462af20a0b3e85ec7df38a90ebf4-5dfc58bd9-dq5hx.out
# export to spark/logs by clicking the export logs button from the provided guide

BROKER_URL = "localhost:9092"  # "localhost:9092" , "kafka:19092"

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redis_server_message_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("ch", BooleanType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType()),
            ])
        ))
    ]
)

# TO-DO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customer_message_schema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

# TO-DO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
stedi_event_message_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

#TO-DO: create a spark application object
spark = SparkSession.builder.appName("stedi-final-events").getOrCreate()

#TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redis_server_raw_streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "redis-server").option("startingOffsets", "earliest").load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
redis_server_streaming_df = redis_server_raw_streaming_df.selectExpr("cast(value as string) value")

# TO-DO:; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
#
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
redis_server_streaming_df.withColumn("value", from_json("value", redis_server_message_schema)) \
    .select(col("value.*")).createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encoded_customer_df = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
decoded_customer_df = encoded_customer_df \
    .withColumn("encodedCustomer", unbase64(encoded_customer_df.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decoded_customer_df.withColumn("customer", from_json("encodedCustomer", customer_message_schema)) \
    .select(col("customer.*")).createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
email_and_birthday_sql_statement = "select email, birthDay from CustomerRecords where email is not null And birthDay is not null"
email_and_birthday_streaming_df = spark.sql(email_and_birthday_sql_statement)

# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
email_and_birth_year_streaming_df = email_and_birthday_streaming_df \
    .select("email", split(email_and_birthday_streaming_df.birthDay, "-") \
            .getItem(0).alias("birthYear"))

# email_and_birth_year_streaming_df = email_and_birthday_streaming_df.select(col('email'), col('birthYear'))


# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
spark = SparkSession.builder.appName("kafka-events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stedi_events_raw_streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "stedi-events").option("startingOffsets", "earliest").load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
stedi_events_streaming_df = stedi_events_raw_streaming_df.selectExpr("cast(key as string) key", "cast(value as string) value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
stedi_events_streaming_df.withColumn("value", from_json("value", stedi_event_message_schema)) \
    .select(col("value.*")).createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customer_risk_streaming_df = spark.sql("select customer, score from CustomerRisk")

email_and_birth_year_streaming_df.createOrReplaceTempView("email_birth_year")
customer_risk_streaming_df.createOrReplaceTempView("customer_risk")


joined_stedi_df = spark.sql("""
    SELECT *
    FROM customer_risk
    JOIN email_birth_year ON customer_risk.customer = email_birth_year.email
""")

# TO-DO: join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
# joined_stedi_df = email_and_birth_year_streaming_df.join(customer_risk_streaming_df, expr("""CustomerRisk.customer=email_and_birth_year_streaming_df.email"""))
# TO-DO: sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}
# joined_stedi_df.select("customer", "score", "email", "birthYear") \
joined_stedi_df \
 .selectExpr("cast(customer as string) key", "to_json(struct(*)) as value") \
 .writeStream.format("kafka") \
 .option("kafka.bootstrap.servers", BROKER_URL) \
 .option("topic", "stedi-risk-scores-final") \
 .option("FailOnDataLoss" , "false") \
 .option("checkpointLocation", "/tmp/kafkacheckpoint").start().awaitTermination()

# check with kafka-console-consumer --bootstrap-server localhost:9092 --topic stedi-risk-scores-final
