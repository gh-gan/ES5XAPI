package com.hna.es.entity;

public enum AggregationType {
    COUNT("count"),/* UNIQUECOUNT("uniquecount"),*/ SUM("sum"), AVG("avg"), MIN("min"), MAX("max");

    private String comment;

    AggregationType(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }
}
