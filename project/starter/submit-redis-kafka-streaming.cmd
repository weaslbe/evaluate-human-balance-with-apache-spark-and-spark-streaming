docker exec -it nd029-c2-apache-spark-and-spark-streaming-starter-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/project/starter/sparkpyrediskafkastreamtoconsole.py | tee ../../spark/logs/redis-kafka.log