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
                .set("spark.app.id", "spark-nft-value-prop-processor");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the value propositions for the NFTs : " + spark);

        //find total trades for each collection
        NFTValueProposition.findNFTsValueProposition(spark);

        spark.stop();

    }

}
