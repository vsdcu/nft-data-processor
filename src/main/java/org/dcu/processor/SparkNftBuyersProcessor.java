package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

/**
 * Create table with the total sum of transference by Buyer
 *
 * Ether value based on 10^18 that is the value of 1 wei
 *
 * https://ethereum.org/en/glossary/#wei
 * https://www.alchemy.com/gwei-calculator
 */
public class SparkNftBuyersProcessor {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
        Dataset<Row> originTransfersDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_TRANSFERS, moralisConnectionManager.getProps())
                .select("to_address", "value");


        originTransfersDataset.show();

        Dataset<Row> result = originTransfersDataset.groupBy("to_address")
                .agg(sum(col("value")).alias("total"))
                .withColumnRenamed("to_address", "buyer_address")
                .withColumnRenamed("total", "total_value")
                .withColumn("total_ether", col("total_value").divide(Math.pow(10, 18)));


        result.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        result.write()
                .mode(SaveMode.Append)
                .option("createTableColumnTypes", "buyer_address varchar(255), total_value DOUBLE, total_ether DOUBLE")
                .jdbc(dcuSparkConnectionManager.getUrl(), "total_transferred_by_buyers", dcuSparkConnectionManager.getProps());

    }
}



