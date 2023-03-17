package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionTrades;

public class SparkTradesProcessor {

    public static void main(String[] args) {

        //96
        //1g
        //240
        //120
        //1g
        //0.2

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Trade-Processor-Job")
                .set("spark.app.id", "spark-trade-processor")
                .set("spark.executor.instances", "16")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "512m")
                .set("spark.default.parallelism", "256")
                .set("spark.sql.shuffle.partitions", "256");

//                .set("spark.executor.instances", args[0])
//                .set("spark.executor.memory", args[1])
//                .set("spark.default.parallelism", args[2])
//                .set("spark.sql.shuffle.partitions", args[3])
//                .set("spark.driver.maxResultSize", args[4])
//                .set("spark.storage.memoryFraction", args[5]);
//
//        System.out.println("*********** Using optimization params as ************");
//        System.out.println("spark.executor.instances: "+args[0]);
//        System.out.println("spark.executor.memory: "+args[1]);
//        System.out.println("spark.default.parallelism: "+args[2]);
//        System.out.println("spark.sql.shuffle.partitions: "+ args[3]);
//        System.out.println("spark.driver.maxResultSize: "+ args[4]);
//        System.out.println("spark.storage.memoryFraction: "+ args[5]);



        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the trades metrics mrtc : " + spark);

        //find total trades for each collection
        //CollectionTrades.findTotalTradesByNFTCollection(spark);

        //find total trades for each token-id present in collection
        //CollectionTrades.findTotalTradesByTokenIdInNFTCollection(spark);

        CollectionTrades.findTrends(spark);

        spark.stop();

    }

}
