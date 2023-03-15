package org.dcu.processor;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;
import org.dcu.json.NftContractJson;
import org.dcu.mapper.NftContractMapper;

import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_CONTRACTS;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

public class SparkNftContractDataProcessor {

    private static final String WRKR_EXECUTOR_MEMORY = "4g";

    private static Gson gson = new Gson();

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .set("spark.executor.memory", args[0])
                .set("spark.sql.shuffle.partitions", args[1])
                .set("spark.driver.maxResultSize", args[2])
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false");

        System.out.println("*********** Using optimization params as ************");
        System.out.println("spark.executor.memory: "+args[0]);
        System.out.println("spark.sql.shuffle.partitions: "+args[1]);
        System.out.println("spark.driver.maxResultSize: "+args[2]);

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


        //nftEntityDataset.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        nftEntityDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "nft_contract_entity",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }
}



