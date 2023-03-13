package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.TopTraders;

public class SparkDataProcessor {

    public static void main(String[] args) {

        String SPARK_HOME = "/Users/vinit/spark_home/spark-3.3.2-bin-hadoop3";

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("NFT-Data_Processor")
                .set("spark.app.id", "spark-nft-processor");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        System.out.println(">>>> Spark session : " + spark);

        //find top buyers
        TopTraders.findTopBuyers(spark);

        //find top sellers
        //TopTraders.findTopSellers(spark);

        //find total trades for each collection
        //CollectionTrades.findTotalTradesByNFTCollection(spark);

        //find total trades for each token-id present in collection
        //CollectionTrades.findTotalTradesByTokenIdInNFTCollection(spark);

        spark.stop();

    }






}
