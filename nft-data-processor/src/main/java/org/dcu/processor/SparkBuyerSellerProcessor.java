package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.TopTraders;

public class SparkBuyerSellerProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()

// my mac settings
                .setAppName("Buyers-Sellers-Processor-Job")
                .set("spark.app.id", "spark-nft-buyer-seller")
                .set("spark.executor.instances", "6")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "10g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "2g");

// my cloud settings
//                .set("spark.executor.instances", "4")
//                .set("spark.executor.cores", "4")
//                .set("spark.executor.memory", "4g")
//                .set("spark.default.parallelism", "32028")
//                .set("spark.sql.shuffle.partitions", "32028");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the top buyers and sellers metrics : " + spark);

        //find top buyers
        //TopTraders.findTopBuyers(spark);

        //find top sellers
        //TopTraders.findTopSellers(spark);

        TopTraders.findTopBuyersSellers(spark);

        spark.stop();

    }

}
