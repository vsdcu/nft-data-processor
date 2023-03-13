package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionTrades;

public class SparkTradesProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Trade-Processor-Job")
                .set("spark.app.id", "spark-trade-processor");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the trades metrics : " + spark);

        //find total trades for each collection
        CollectionTrades.findTotalTradesByNFTCollection(spark);

        //find total trades for each token-id present in collection
        CollectionTrades.findTotalTradesByTokenIdInNFTCollection(spark);

        spark.stop();

    }

}
