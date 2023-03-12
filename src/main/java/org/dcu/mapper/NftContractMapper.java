package org.dcu.mapper;

import com.google.gson.Gson;
import org.dcu.json.NftContractJson;

public class NftContractMapper {

    static Gson gson = new Gson();

    public static NftContractJson map(String json) {
        return gson.fromJson(json, NftContractJson.class);
    }
}
