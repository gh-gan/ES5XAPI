package com.hna.es.entity;

public enum Operator {
    GT(">"), GTE(">="), EQ("=="), LT("<"), LTE("<=");

    private String symbol;

    Operator(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
}
