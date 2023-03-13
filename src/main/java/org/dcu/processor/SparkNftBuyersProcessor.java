package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;
import org.dcu.json.NftContractJson;
import org.dcu.mapper.NftContractMapper;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import static org.dcu.database.MoralisConnectionManager.*;

public class SparkNftBuyersProcessor {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
        Dataset<Row> originTransfersDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_TRANSFERS, moralisConnectionManager.getProps())
                .select("to_address", "value");


        originTransfersDataset.show();


        Dataset<Row> result = originTransfersDataset.groupBy("to_address")
                .agg(sum(col("value")).alias("total"))
                .withColumnRenamed("to_address", "buyer_address")
                .withColumnRenamed("total", "total_value");


        result.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        result.write()
                .mode(SaveMode.Append)
                .jdbc(dcuSparkConnectionManager.getUrl(), "transfer_buyers_sum_value", dcuSparkConnectionManager.getProps());

    }
}



