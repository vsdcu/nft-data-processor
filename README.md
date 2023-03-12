# nft-data-processor

Prerequisite
--------------
Install spark libs and have a standalone or cluster setup running spark master/workers

command to run the code -
-----------------------------------------
./bin/spark-submit --master spark://vinits-MBP:7077 --class <main-class> --jars <required-libs> <application-jar>

example command to run the code - 
-----------------------------------------
./bin/spark-submit --master spark://vinits-MBP:7077 
    --class org.dcu.processor.SparkDataProcessor 
    --jars ~/spark-workspace-link/mysql-connector-j-8.0.32.jar 
    ~/spark-workspace-link/nft-data-processor/target/spark-data-processor-1.0-SNAPSHOT.jar