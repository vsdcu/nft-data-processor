package org.dcu.processor;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.dcu.database.DcuSparkConnectionManager.TABLE_NFT_CONTRACT_ENTITY;

/*
Show the basic information for a collection like Name and Symbol
 */
public class SparkNftCollectionNameSymbol {


    public static void main(String[] args) {

        System.out.println("Spark job to collect nft-info again");

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-collection-info")

        // vsdcu mac settings
                .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "4g")
                .set("spark.default.parallelism", "256")
                .set("spark.sql.shuffle.partitions", "256");

//                .setAppName("Buyers-Sellers-Processor-Job")
//                .set("spark.app.id", "spark-nft-buyer-seller")
//                .set("spark.executor.instances", "6")
//                .set("spark.executor.cores", "4")
//                .set("spark.executor.memory", "10g")
//                .set("spark.default.parallelism", "24")
//                .set("spark.sql.shuffle.partitions", "128")
//                .set("spark.driver.maxResultSize", "2g");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();


        Dataset<Row> countDS = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                "full_parsed_nft_contract_data", moralisConnectionManager.getProps()).repartition(col("nft_address"))
                .select("nft_address", "name", "symbol")
                .groupBy("nft_address", "name", "symbol")
                .count()
                .orderBy(desc("count"));

        //countDS.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        countDS.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "new_nft_collection_info",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }
}
