package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.ConnectionManager;

public class SparkDataProcessor {
    public static void main(String[] args) {

        String SPARK_HOME = "/Users/vinit/spark_home/spark-3.3.2-bin-hadoop3";

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application-krys")
                .set("spark.app.id", "spark-nft-reader-krys");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        System.out.println(">>>> Spark session : " + spark);

        // read from GCP MySQL database
        String tableName = "krys_nft_contracts";
        System.out.println(">>>> Table name : " + tableName);
        ConnectionManager connectionManager = new ConnectionManager();
        Dataset<Row> nft_contracts = spark.read().jdbc(connectionManager.getUrl(), tableName, connectionManager.getProps()).select("json_data");
        nft_contracts.show();

        //count data form nft_contracts
        long count = nft_contracts.count();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>.Total records in table {"+tableName+"} : " + count);

        spark.stop();

    }






}
