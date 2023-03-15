package org.dcu.processor;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;
import org.dcu.json.NftContractJson;

import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_CONTRACTS;

public class SparkNftContractDataProcessor {

    private static Gson gson = new Gson();

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .set("spark.driver.memory", "1g")
                .set("spark.executor.memory", "1g")
                .set("spark.sql.shuffle.partitions", "100")
                .set("spark.driver.maxResultSize", "512m")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
        Dataset<Row> originNfttDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_CONTRACTS, moralisConnectionManager.getProps())
                .select("nft_address", "token_address", "token_id", "json_data");

        //originNfttDataset.show();


        Dataset<NftContractJson> nftEntityDataset = originNfttDataset.map(
                (MapFunction<Row, NftContractJson>) row -> {
                    String nftAddress = row.getString(row.fieldIndex("nft_address"));
                    String json = row.getString(row.fieldIndex("json_data"));

                    NftContractJson nftEntity = gson.fromJson(json, NftContractJson.class);

                    nftEntity.setNftAddress(nftAddress);

                    return nftEntity;
                },
                Encoders.bean(NftContractJson.class)
        );


//        nftEntityDataset.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        nftEntityDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "nft_contract_entity",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }
}



