package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionMinters;

public class SparkMintersProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Minters-Processor-Job")
                .set("spark.app.id", "spark-minters-processor")
                .set("spark.executor.instances", "6")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "10g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "2g");

        //vsdcu: my mac setup
/*              .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "6g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "1g");*/


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the Minters metrics : " + spark);

        CollectionMinters.findTopMintersAndTopMintedCollections(spark);

        spark.stop();

    }

}
