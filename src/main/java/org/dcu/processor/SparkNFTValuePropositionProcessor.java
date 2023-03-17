package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionTrades;
import org.dcu.datacollector.NFTValueProposition;

public class SparkNFTValuePropositionProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("NFT-Value-Proposition-Job")
                .set("spark.app.id", "spark-nft-value-prop-processor")
//                .set("spark.executor.instances", "2")
//                .set("spark.executor.cores", "2")
//                .set("spark.executor.memory", "6g")
//                .set("spark.default.parallelism", "32028")
//                .set("spark.sql.shuffle.partitions", "32028");

// my mac settings
                .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "6500m")
                .set("spark.default.parallelism", "32028")
                .set("spark.sql.shuffle.partitions", "32028")
                .set("spark.driver.maxResultSize", "1g");

//        System.out.println("*********** Using optimization params as ************");
//        System.out.println("spark.executor.memory: "+args[0]);
//        System.out.println("spark.sql.shuffle.partitions: "+args[1]);
//        System.out.println("spark.driver.maxResultSize: "+args[2]);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the value propositions for the NFTs : " + spark);

        //find total trades for each collection
        NFTValueProposition.findNFTsValueProposition(spark);

        spark.stop();

    }

}
