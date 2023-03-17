package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.TopTraders;

public class SparkBuyerSellerProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Buyers-Sellers-Processor-Job")
                .set("spark.app.id", "spark-nft-buyer-seller")
//                .set("spark.shuffle.service.enabled", "true")
//                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "4g")
                .set("spark.default.parallelism", "32028")
                .set("spark.sql.shuffle.partitions", "32028");


//                .set("spark.executor.memory", args[0])
//                .set("spark.sql.shuffle.partitions", args[1])
//                .set("spark.driver.maxResultSize", args[2]);
//
//        System.out.println("*********** Using optimization params as ************");
//        System.out.println("spark.executor.memory: "+args[0]);
//        System.out.println("spark.sql.shuffle.partitions: "+args[1]);
//        System.out.println("spark.driver.maxResultSize: "+args[2]);

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
