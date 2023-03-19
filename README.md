# nft-data-processor

Prerequisite
--------------
Install spark libs and have a standalone or cluster setup running spark master/workers. I have tested this application successfully on both the environment.
- GCP Dataproc and 
- Standalone installaction of Spark on my macbook-pro 16-cores/32GB, 

command to build the project
---------------------------------------
This is a maven project that uses maven-wrapper, so make sure you have it all installed before building.
./mvnw clean install

deployment on spark cluster
--------------------------------
once the build is successful, you need to package your code in a app;lication.jar that can be run using the spark-submit. 
For this application, you will find spark-data-processor-1.0-SNAPSHOT.jar in your target directory that needs to be shipped to cloud or local standalone spark machine to run the jobs.  

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
- ("spark.executor.instances", "4")
- ("spark.executor.cores", "4")
- ("spark.executor.memory", "6g")
- ("spark.default.parallelism", "24")
- ("spark.sql.shuffle.partitions", "128")
- ("spark.driver.maxResultSize", "1g");    
