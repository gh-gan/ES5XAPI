package com.hna.es.entity;

public class Order {
    private String orderBy;
    private Ordering ordering;
    private AggregationType aggregationType;

    public Order(String orderBy, Ordering ordering) {
        this.orderBy = orderBy;
        this.ordering = ordering;
    }

    public String getOrderBy() {
        return this.orderBy;
    }

    public Ordering getOrdering() {
        return this.ordering;
    }

    public AggregationType getAggregationType() {
        return this.aggregationType;
    }

    public void setAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    public enum Ordering {ESC, DESC}
}
