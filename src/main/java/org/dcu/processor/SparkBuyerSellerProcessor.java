package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.TopTraders;

public class SparkBuyerSellerProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Buyers-Sellers-Processor-Job")
                .set("spark.app.id", "spark-nft-buyer-seller");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the top buyers and sellers metrics : " + spark);

        //find top buyers
        TopTraders.findTopBuyers(spark);

        //find top sellers
        TopTraders.findTopSellers(spark);

        spark.stop();

    }

}
