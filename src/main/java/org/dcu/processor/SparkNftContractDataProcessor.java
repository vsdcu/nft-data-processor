package org.dcu.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dcu.database.MoralisConnectionManager;
import org.dcu.json.NftContractJson;

import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_CONTRACTS;

public class SparkNftContractDataProcessor {

    private static final String WRKR_EXECUTOR_MEMORY = "4g";

    private static Gson gson = new Gson();
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        String memory;
        String partitions;
        String maxResultSize;

        if (args.length > 0) {
            memory = args[0];
            partitions = args[1];
            maxResultSize = args[2];
        } else {
            memory = "1g";
            partitions = "1073741824";
            maxResultSize = "512m";
        }


        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .set("spark.executor.memory", memory)
                .set("spark.sql.shuffle.partitions", partitions)
                .set("spark.driver.maxResultSize", maxResultSize)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false");

        System.out.println("*********** Using optimization params as ************");
        System.out.println("spark.executor.memory: " + memory);
        System.out.println("spark.sql.shuffle.partitions: " + partitions);
        System.out.println("spark.driver.maxResultSize: " + maxResultSize);

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

                    NftContractJson nftEntity = parseNftContractJsonWithJackson(json, nftAddress);


                    return nftEntity;
                },
                Encoders.bean(NftContractJson.class)
        );


        //nftEntityDataset.show();

        MoralisConnectionManager dcuSparkConnectionManager = new MoralisConnectionManager();
        nftEntityDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dcuSparkConnectionManager.getUrl(),
                        "nft_contract_entity",
                        dcuSparkConnectionManager.getProps());

        sparkSession.stop();
    }

    private static NftContractJson parseNftContractJsonWithGson(String jsonString, String nftAddress) {
        NftContractJson nftEntity = gson.fromJson(jsonString, NftContractJson.class);
        nftEntity.setNftAddress(nftAddress);
        return nftEntity;
    }


    private static NftContractJson parseNftContractJsonWithJackson(String jsonString, String nftAddress) throws JsonProcessingException {

        JsonNode rootNode = mapper.readTree(jsonString);

        return NftContractJson.builder()
                .nftAddress(nftAddress)
                .tokenHash(rootNode.get("token_hash").asText())
                .tokenAddress(rootNode.get("token_address").asText())
                .tokenId(rootNode.get("token_id").asText())
                .amount(rootNode.get("amount").asText())
                .blockNumberMinted(rootNode.get("block_number_minted").asText())
                .contractType(rootNode.get("contract_type").asText())
                .name(rootNode.get("name").asText())
                .symbol(rootNode.get("symbol").asText())
                .tokenUri(rootNode.get("token_uri").asText())
                .lastTokenUriSync(rootNode.get("last_token_uri_sync").asText())
                .lastMetadataSync(rootNode.get("last_metadata_sync").asText())
                .minterAddress(rootNode.get("minter_address").asText()).build();
    }
}



