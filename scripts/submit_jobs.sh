#!/bin/bash

"$SPARK_HOME"/bin/spark-submit --master spark://vinits-MBP:7077 --class org.dcu.processor.SparkBuyerSellerProcessor --jars ~/spark-workspace-link/mysql-connector-j-8.0.32.jar  ~/spark-workspace-link/nft-data-processor/target/spark-data-processor-1.0-SNAPSHOT.jar

"$SPARK_HOME"/bin/spark-submit --master spark://vinits-MBP:7077 --class org.dcu.processor.SparkTradesProcessor --jars ~/spark-workspace-link/mysql-connector-j-8.0.32.jar  ~/spark-workspace-link/nft-data-processor/target/spark-data-processor-1.0-SNAPSHOT.jar
