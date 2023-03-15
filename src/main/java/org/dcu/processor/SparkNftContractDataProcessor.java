package org.dcu.processor;

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

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .set("spark.executor.memory", args[0]);

        System.out.println("*********** Using spark.executor.memory:"+args[0]);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
        Dataset<Row> originNfttDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_CONTRACTS, moralisConnectionManager.getProps())
                .select("nft_address", "token_address", "token_id", "json_data");

        //originNfttDataset.show();

        Dataset<NftContractJson> nftEntityDataset = originNfttDataset.map(
                (MapFunction<Row, NftContractJson>) row -> {
                    String json = row.getString(row.fieldIndex("json_data"));

                    NftContractJson nftEntity = NftContractMapper.map(json);
                    nftEntity.setNftAddress(row.getString(row.fieldIndex("nft_address")));

                    return nftEntity;
                },
                Encoders.bean(NftContractJson.class)
        );


        //nftEntityDataset.show();

        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();
        nftEntityDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(), "nft_contract_entity", dcuSparkConnectionManager.getProps());

    }
}



