spark-submit --master yarn --class com.github.sharpdata.sharpetl.spark.Entrypoint hdfs://master:9000/etl/spark.jar batch-job --names=report_1,report_2,report_3 --period=1440 --default-start-time="2000-01-01 00:00:00" --property=hdfs://master:9000/etl/etl.properties