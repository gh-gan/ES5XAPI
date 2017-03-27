package com.hna.es.entity;

public class Range {
    private double start;
    private double end;
    private boolean startInclusive;
    private boolean endInclusive;

    public Range(double start, double end) {
        this.start = start;
        this.end = end;
        this.startInclusive = false;
        this.endInclusive = false;
    }

    public Range(double start, double end, boolean startInclusive, boolean endInclusive) {
        this.start = start;
        this.end = end;
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    public double getStart() {
        return start;
    }

    public double getEnd() {
        return end;
    }

}
