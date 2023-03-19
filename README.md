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
    --class org.dcu.processor.SparkBuyerSellerProcessor 
    --jars ~/spark-workspace-link/mysql-connector-j-8.0.32.jar 
    ~/spark-workspace-link/nft-data-processor/target/spark-data-processor-1.0-SNAPSHOT.jar
    
    
external dependency Jars - 
-----------------------------
mysql-connector-j-8.0.32.jar  - To connect to the MySQL database.

    
Mac settings used to run on the standalone system (You need to tweak it as per your machine hardware)
----------------------------------------------------------------------------------------------------------
("spark.executor.instances", "4")
("spark.executor.cores", "4")
("spark.executor.memory", "6g")
("spark.default.parallelism", "24")
("spark.sql.shuffle.partitions", "128")
("spark.driver.maxResultSize", "1g");    
