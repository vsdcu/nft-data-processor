package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.*;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

/**
 * Create table with the total sum of transference by Seller
 *
 * Ether value based on 10^18 that is the value of 1 wei
 *
 * https://ethereum.org/en/glossary/#wei
 * https://www.alchemy.com/gwei-calculator
 */
public class SparkNftSellersProcessor {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", SparkNftBuyersProcessor.class.getName())
                .set("spark.executor.memory", args[0])
                .set("spark.sql.shuffle.partitions", args[1])
                .set("spark.driver.maxResultSize", args[2]);

        System.out.println("*********** Using optimization params as ************");
        System.out.println("spark.executor.memory: "+args[0]);
        System.out.println("spark.sql.shuffle.partitions: "+args[1]);
        System.out.println("spark.driver.maxResultSize: "+args[2]);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
        Dataset<Row> originTransfersDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_TRANSFERS, moralisConnectionManager.getProps())
                .select("from_address", "value");


        //originTransfersDataset.show();

        Dataset<Row> result = originTransfersDataset.groupBy("from_address")
                .agg(sum(col("value")).alias("total"))
                .withColumnRenamed("from_address", "seller_address")
                .withColumnRenamed("total", "total_value")
                .withColumn("total_ether", col("total_value").divide(Math.pow(10, 18)));


        //result.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        result.write()
                .mode(SaveMode.Overwrite)
                .option("createTableColumnTypes", "seller_address varchar(255), total_value DOUBLE, total_ether DOUBLE")
                .jdbc(dcuSparkConnectionManager.getUrl(), "total_transferred_by_sellers", dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }
}



