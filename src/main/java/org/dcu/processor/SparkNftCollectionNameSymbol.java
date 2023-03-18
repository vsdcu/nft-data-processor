package org.dcu.processor;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.desc;
import static org.dcu.database.DcuSparkConnectionManager.TABLE_NFT_CONTRACT_ENTITY;

/*
Show the basic information for a collection like Name and Symbol
 */
public class SparkNftCollectionNameSymbol {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-collection-info");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();


        Dataset<Row> countDS = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                "mrc_parsed_nft_contract_data", moralisConnectionManager.getProps())
                .select("nft_address", "name", "symbol")
                .groupBy("nft_address", "name", "symbol")
                .count()
                .orderBy(desc("count"));

        countDS.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        countDS.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "nft_collection_info",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }
}
