package org.dcu.json;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class MetadataJson {

    private String name;

    private String description;

    private Object animationUrl;

    @SerializedName("external_link")
    private Object externalLink;

    private String image;

    private List<Map<String, String>> attributes;

}
