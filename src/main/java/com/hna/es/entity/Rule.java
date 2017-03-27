package com.hna.es.entity;

public class Rule {
    private AggregationType type;
    private Operator operator;
    private double threshold;

    public Rule(AggregationType type, Operator operator, double threshold) {
        this.type = type;
        this.operator = operator;
        this.threshold = threshold;
    }

    public String toString() {
        return this.type.getComment() + ":" + this.operator.getSymbol() + this.threshold;
    }

    public AggregationType getType() {
        return type;
    }

    public Operator getOperator() {
        return operator;
    }

    public double getThreshold() {
        return threshold;
    }
}
