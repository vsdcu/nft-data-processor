package org.dcu.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class NftContractJson {

    private String nftAddress;

    @SerializedName("token_address")
    private String tokenAddress;

    @SerializedName("token_id")
    private String tokenId;

    private String amount;

    @SerializedName("token_hash")
    private String tokenHash;

    @SerializedName("block_number_minted")
    private String blockNumberMinted;
    @SerializedName("updated_at")
    private String updatedAt;

    @SerializedName("contract_type")
    private String contractType;

    private String name;

    private String symbol;
    @SerializedName("token_uri")

    private String tokenUri;
    @SerializedName("last_token_uri_sync")

    private String lastTokenUriSync;
    @SerializedName("last_metadata_sync")

    private String lastMetadataSync;

    @SerializedName("minter_address")
    private String minterAddress;

    // Normalized_metadata
//    @JsonProperty("normalized_metadata")
//    @SerializedName("normalized_metadata")
//    private MetadataJson metadata;

}
