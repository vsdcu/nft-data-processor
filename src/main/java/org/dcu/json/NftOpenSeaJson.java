package org.dcu.json;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.List;

@Data
public class NftOpenSeaJson {


    private String nftAddress;

    @SerializedName("transaction_hash")
    private String transactionHash;

    @SerializedName("transaction_index")
    private String transactionIndex;

    @SerializedName("token_ids")
    private List<String> tokenIds;

    @SerializedName("seller_address")
    private String sellerAddress;

    @SerializedName("buyer_address")
    private String buyerAddress;

    @SerializedName("token_address")
    private String tokenAddress;

    @SerializedName("marketplace_address")
    private String marketplaceAddress;
    private String price;

    @SerializedName("price_token_address")
    private String priceTokenAddress;

    @SerializedName("block_timestamp")
    private String blockTimestamp;

    @SerializedName("block_number")
    private String blockNumber;

    @SerializedName("block_hash")
    private String blockHash;



}
