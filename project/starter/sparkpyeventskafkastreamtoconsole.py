from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

BROKER_URL = "localhost:9092"  # "localhost:9092" , "kafka:19092"
# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
spark = SparkSession.builder.appName("kafka-events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stedi_events_raw_streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "stedi-events").option("startingOffsets", "earliest").load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
stedi_events_streaming_df = stedi_events_raw_streaming_df.selectExpr("cast(value as string) value")

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

stedi_event_message_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)
stedi_events_streaming_df.withColumn("value", from_json("value", stedi_event_message_schema)) \
    .select(col("value.*")).createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view,
# selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customer_risk_streaming_df = spark.sql("SELECT customer, score FROM CustomerRisk")

# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
customer_risk_streaming_df.writeStream.outputMode("append").format("console").start().awaitTermination()


# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafkastreaming.sh
# Verify the data looks correct 