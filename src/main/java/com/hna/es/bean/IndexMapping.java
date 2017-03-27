package com.hna.es.bean;

import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.util.List;

/**
 * Created by GH-GAN on 2016/12/20.
 */
public class IndexMapping implements Serializable {
    List<IndexTypeProperties> type_fileds;

    public List<IndexTypeProperties> getType_fileds() {
        return type_fileds;
    }

    public void setType_fileds(List<IndexTypeProperties> type_fileds) {
        this.type_fileds = type_fileds;
    }
}
