package org.dcu.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;
import org.dcu.json.NftContractJson;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_CONTRACTS;

public class SparkNftContractDataProcessor {

    private static String INSERT_QUERY = "INSERT INTO mrc_nft_contract_entity (nftAddress, tokenAddress, tokenId, amount, tokenHash, blockNumberMinted, updatedAt, contractType, name, symbol, tokenUri, lastTokenUriSync, lastMetadataSync, minterAddress) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";

    private static int INSERT_BATCH_SIZE = 10000;

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
            memory = "512m";
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
                .set("spark.kryo.registrationRequired", "false")

                // Configure GC algorithm
                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

                // Tune GC settings
                .set("spark.executor.extraJavaOptions", "-XX:NewRatio=3 -XX:MaxTenuringThreshold=15 -XX:SurvivorRatio=8")
                .set("spark.driver.extraJavaOptions", "-XX:NewRatio=3 -XX:MaxTenuringThreshold=15 -XX:SurvivorRatio=8");


        System.out.println("*********** Using optimization params as ************");
        System.out.println("spark.executor.memory: " + memory);
        System.out.println("spark.sql.shuffle.partitions: " + partitions);
        System.out.println("spark.driver.maxResultSize: " + maxResultSize);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();


        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();

        Dataset<Row> originNfttDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_CONTRACTS, moralisConnectionManager.getProps())
                .select("nft_address", "json_data")
                .repartition(100);

        //originNfttDataset.show();

        Dataset<NftContractJson> nftEntityDataset = originNfttDataset.map(
                (MapFunction<Row, NftContractJson>) row -> {
                    String nftAddress = row.getString(row.fieldIndex("nft_address"));
                    String json = row.getString(row.fieldIndex("json_data"));

                    NftContractJson nftEntity = parseNftContractJsonWithJackson(json, nftAddress,  sparkSession.log());


                    return nftEntity;
                },
                Encoders.bean(NftContractJson.class)
        );


        //nftEntityDataset.show();


        DcuSparkConnectionManager dcuSparkConnectionManager = new DcuSparkConnectionManager();

        // New code for batch insert
        writeToDatabase(nftEntityDataset, dcuSparkConnectionManager);

        sparkSession.stop();
    }

    public static void writeToDatabase(Dataset<NftContractJson> nftEntityDataset, DcuSparkConnectionManager dcuSparkConnectionManager) {
        String url = dcuSparkConnectionManager.getUrl();
        Properties props = dcuSparkConnectionManager.getProps();
        nftEntityDataset.foreachPartition(partition -> {
            Connection connection = DriverManager.getConnection(url, props);
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_QUERY);


            int currentBatchSize = 0;

            while (partition.hasNext()) {
                NftContractJson nftEntity = partition.next();
                if(nftEntity == null) {
                    continue;
                }

                preparedStatement.setObject(1, nftEntity.getNftAddress());
                preparedStatement.setObject(2, nftEntity.getTokenAddress());
                preparedStatement.setObject(3, nftEntity.getTokenId());
                preparedStatement.setObject(4, nftEntity.getAmount());
                preparedStatement.setObject(5, nftEntity.getTokenHash());
                preparedStatement.setObject(6, nftEntity.getBlockNumberMinted());
                preparedStatement.setObject(7, nftEntity.getUpdatedAt());
                preparedStatement.setObject(8, nftEntity.getContractType());
                preparedStatement.setObject(9, nftEntity.getName());
                preparedStatement.setObject(10, nftEntity.getSymbol());
                preparedStatement.setObject(11, nftEntity.getTokenUri());
                preparedStatement.setObject(12, nftEntity.getLastTokenUriSync());
                preparedStatement.setObject(13, nftEntity.getLastMetadataSync());
                preparedStatement.setObject(14, nftEntity.getMinterAddress());


                preparedStatement.addBatch();
                currentBatchSize++;

                if (currentBatchSize >= INSERT_BATCH_SIZE) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    currentBatchSize = 0;
                }
            }

            if (currentBatchSize > 0) {
                preparedStatement.executeBatch();
                connection.commit();
            }

            preparedStatement.close();
            connection.close();
        });
    }


    private static NftContractJson parseNftContractJsonWithJackson(String jsonString, String nftAddress, Logger log) throws JsonProcessingException {

        try {
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
        } catch (Exception e) {
            log.error("Invalid JSON Format. {}", jsonString);
            return null;
        }
    }
}



