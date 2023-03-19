package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.NFTValueProposition;

public class SparkNFTValuePropositionProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("NFT-Value-Proposition-Job")
                .set("spark.app.id", "spark-nft-value-prop-processor")
                .set("spark.executor.instances", "6")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "10g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "2g");

// my mac settings
//                .set("spark.executor.instances", "4")
//                .set("spark.executor.cores", "4")
//                .set("spark.executor.memory", "6500m")
//                .set("spark.default.parallelism", "32028")
//                .set("spark.sql.shuffle.partitions", "32028")
//                .set("spark.driver.maxResultSize", "1g");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the value propositions for the NFTs : " + spark);

        //find total trades for each collection
        NFTValueProposition.findNFTsValueProposition(spark);

        spark.stop();

    }

}
