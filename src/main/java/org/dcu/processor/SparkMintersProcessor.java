package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dcu.datacollector.CollectionMinters;
import org.dcu.datacollector.CollectionTrades;

public class SparkMintersProcessor {

    public static void main(String[] args) {

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Minters-Processor-Job")
                .set("spark.app.id", "spark-minters-processor")
                .set("spark.executor.memory", args[0]);

        System.out.println("*********** Using spark.executor.memory:"+args[0]);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        System.out.println(">>>> Job to find the Minters metrics : " + spark);

        CollectionMinters.findTopMinters(spark);

        spark.stop();

    }

}
