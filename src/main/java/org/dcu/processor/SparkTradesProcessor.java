package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionTrades;

/**
 * Main class to call the spark job to find the trends in NFT trades happened over time.
 * Calls the data-collector class SparkTradesProcessor
 */
public class SparkTradesProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Trade-Processor-Job")
                .set("spark.app.id", "spark-trade-processor")

// my mac settings
                .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "6500m")
                .set("spark.default.parallelism", "16014")
                .set("spark.sql.shuffle.partitions", "16014")
                .set("spark.driver.maxResultSize", "1g");

//                .set("spark.executor.instances", args[0])
//                .set("spark.executor.memory", args[1])
//                .set("spark.default.parallelism", args[2])
//                .set("spark.sql.shuffle.partitions", args[3])
//                .set("spark.driver.maxResultSize", args[4])
//                .set("spark.storage.memoryFraction", args[5]);

//        System.out.println("*********** Using optimization params as ************");
//        System.out.println("spark.executor.instances: "+args[0]);
//        System.out.println("spark.executor.memory: "+args[1]);
//        System.out.println("spark.default.parallelism: "+args[2]);
//        System.out.println("spark.sql.shuffle.partitions: "+ args[3]);
//        System.out.println("spark.driver.maxResultSize: "+ args[4]);
//        System.out.println("spark.storage.memoryFraction: "+ args[5]);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the trades metrics: " + spark);

        CollectionTrades.findTrends(spark);

        spark.stop();

    }

}
