package com.hna.es.bean;

import org.elasticsearch.search.SearchHit;

import java.io.Serializable;

/**
 * Created by GH-GAN on 2016/12/20.
 */
public class PagingResult implements Serializable {
    long total;
    SearchHit[] hits;

    public PagingResult() {    }

    public PagingResult(long total, SearchHit[] hits) {
        this.total = total;
        this.hits = hits;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public SearchHit[] getHits() {
        return this.hits;
    }

    public void setHits(SearchHit[] hits) {
        this.hits = hits;
    }
}
