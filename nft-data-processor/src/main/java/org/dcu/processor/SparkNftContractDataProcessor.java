package org.dcu.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

    private static String INSERT_QUERY = "INSERT INTO nft_contract_entity (nftAddress, tokenAddress, tokenId, amount, tokenHash, blockNumberMinted, updatedAt, contractType, name, symbol, tokenUri, lastTokenUriSync, lastMetadataSync, minterAddress) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";

    private static int INSERT_BATCH_SIZE = 5000;

    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Copy and Parse NftContract to DCU_Spark schema")
                .set("spark.app.id", "spark-nft-contract-parse")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false")
// vsdcu mac settings
                .setAppName("Buyers-Sellers-Processor-Job")
                .set("spark.app.id", "spark-nft-buyer-seller")
                .set("spark.executor.instances", "6")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "10g")
                .set("spark.default.parallelism", "24")
                .set("spark.sql.shuffle.partitions", "128")
                .set("spark.driver.maxResultSize", "2g");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();

        Dataset<Row> originNfttDataset = sparkSession.read().jdbc(moralisConnectionManager.getUrl(),
                TABLE_NFT_CONTRACTS, moralisConnectionManager.getProps())
                .select("nft_address", "json_data")
                .repartition(100);

        Dataset<NftContractJson> nftEntityDataset = originNfttDataset.map(
                (MapFunction<Row, NftContractJson>) row -> {
                    String nftAddress = row.getString(row.fieldIndex("nft_address"));
                    String json = row.getString(row.fieldIndex("json_data"));

                    NftContractJson nftEntity = parseNftContractJsonWithJackson(json, nftAddress, sparkSession.log());


                    return nftEntity;
                },
                Encoders.bean(NftContractJson.class)
        );

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
                if (nftEntity == null) {
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



