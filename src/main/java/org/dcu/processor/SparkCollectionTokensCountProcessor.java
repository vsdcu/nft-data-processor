package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;

import static org.apache.spark.sql.functions.desc;
import static org.dcu.database.DcuSparkConnectionManager.TABLE_NFT_CONTRACT_ENTITY;


/**
 * Count the Tokens per NFT_ADDRESS
 */
public class SparkCollectionTokensCountProcessor {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .setAppName("Buyers-Sellers-Processor-Job")
                .set("spark.app.id", "spark-nft-buyer-seller")
                .set("spark.executor.instances", "6")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "10g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "2g");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();


        Dataset<Row> countDS = sparkSession.read().jdbc(dcuSparkConnectionManager.getUrl(),
                TABLE_NFT_CONTRACT_ENTITY, dcuSparkConnectionManager.getProps())
                .select("nftAddress", "name", "symbol")
                .groupBy("nftAddress", "name", "symbol")
                .count()
                .orderBy(desc("count"));;


        countDS.show();


        countDS.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "nft_collection_token_count",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }

}
