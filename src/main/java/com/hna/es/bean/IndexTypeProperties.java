package com.hna.es.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by GH-GAN on 2016/12/20.
 */
public class IndexTypeProperties implements Serializable {
    String type;
    Map<String, Map<String, Object>> properties;   // Map<filed, Map<filed2, type>>

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Map<String, Object>> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Map<String, Object>> properties) {
        this.properties = properties;
    }
}
