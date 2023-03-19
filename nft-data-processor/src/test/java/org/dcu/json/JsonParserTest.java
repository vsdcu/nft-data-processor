package org.dcu.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonParserTest {

    private Gson gson = new Gson();

    private String getJson(String s) throws IOException {
        Path path = Paths.get(s);
        return Files.toString(path.toFile(), Charset.defaultCharset());
    }


    @Test
    void parseNftContractJackson() throws IOException {

        String jsonString = getJson("src/test/resources/nft_metadata_multiple_attributes.json");



        NftContractJson jackNft = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonString);

            jackNft = new NftContractJson.NftContractJsonBuilder()
//            .nftAddress("")
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
            e.printStackTrace();
        }


        NftContractJson nftContractJson = gson.fromJson(jsonString, NftContractJson.class);

        assertEquals(jackNft, nftContractJson);

    }

    @Test
    void parseNftContractNoAttributes() {

        try {
            String json = getJson("src/test/resources/nft_metadata_empty_attributes.json");
            System.out.println(json);

            NftContractJson nftContractJson = gson.fromJson(json, NftContractJson.class);

            assertNotNull(nftContractJson);
            assertNotNull(nftContractJson.getTokenAddress());
            assertNotNull(nftContractJson.getTokenId());
            assertNotNull(nftContractJson.getAmount());
            assertNotNull(nftContractJson.getTokenHash());
            assertNotNull(nftContractJson.getBlockNumberMinted());
            assertNotNull(nftContractJson.getContractType());
            assertNotNull(nftContractJson.getName());
            assertNotNull(nftContractJson.getSymbol());
            assertNotNull(nftContractJson.getTokenUri());
            assertNotNull(nftContractJson.getMinterAddress());

//            assertNotNull(nftContractJson.getMetadata());
//            assertNotNull(nftContractJson.getMetadata().getName());
//            assertNotNull(nftContractJson.getMetadata().getDescription());
//            assertNotNull(nftContractJson.getMetadata().getImage());



        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void parseNftContractMultipleAttributes() {

        try {
            String json = getJson("src/test/resources/nft_metadata_multiple_attributes.json");

            NftContractJson nftContractJson = gson.fromJson(json, NftContractJson.class);

            assertNotNull(nftContractJson);
            assertNotNull(nftContractJson.getTokenAddress());
            assertNotNull(nftContractJson.getTokenId());
            assertNotNull(nftContractJson.getAmount());
            assertNotNull(nftContractJson.getTokenHash());
            assertNotNull(nftContractJson.getBlockNumberMinted());
            assertNotNull(nftContractJson.getContractType());
            assertNotNull(nftContractJson.getName());
            assertNotNull(nftContractJson.getSymbol());
            assertNotNull(nftContractJson.getTokenUri());
            assertNotNull(nftContractJson.getMinterAddress());

//            assertNotNull(nftContractJson.getMetadata());
//            assertNotNull(nftContractJson.getMetadata().getName());
//            assertNotNull(nftContractJson.getMetadata().getDescription());
//            assertNotNull(nftContractJson.getMetadata().getImage());
//
//            assertNotNull(nftContractJson.getMetadata().getAttributes());

//            List<Map<String, String>> attributes = nftContractJson.getMetadata().getAttributes();
//            assertAll("List contains attributes",
//                    () -> assertTrue(attributes.stream().anyMatch(m -> m.containsKey("trait_type"))),
//                    () -> assertTrue(attributes.stream().anyMatch(m -> m.get("value").equals("Line Color Options: 4")))
//            );

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }



    @Test
    void parseOpenSeaTrades() {
        try {
            String json = getJson("src/test/resources/nft_open_sea_trade.json");
            NftOpenSeaJson nftOpenSeaJson = gson.fromJson(json, NftOpenSeaJson.class);


            assertNotNull(nftOpenSeaJson.getTransactionHash());
            assertNotNull(nftOpenSeaJson.getTransactionIndex());
            assertNotNull(nftOpenSeaJson.getTokenIds());
            assertNotNull(nftOpenSeaJson.getSellerAddress());
            assertNotNull(nftOpenSeaJson.getBuyerAddress());
            assertNotNull(nftOpenSeaJson.getTokenAddress());
            assertNotNull(nftOpenSeaJson.getMarketplaceAddress());
            assertNotNull(nftOpenSeaJson.getPrice());
            assertNotNull(nftOpenSeaJson.getPriceTokenAddress());
            assertNotNull(nftOpenSeaJson.getBlockTimestamp());
            assertNotNull(nftOpenSeaJson.getBlockNumber());
            assertNotNull(nftOpenSeaJson.getBlockHash());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
