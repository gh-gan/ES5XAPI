package com.hna.es.util;

public class QueryStringBuilders {
    public static String timeRangeQuery(String timeField, long start, long end) {
        // temporarily both inclusive
        String queryTemplate = "%s:[%d TO %d]";
        return String.format(queryTemplate, timeField, start, end);
    }

    public static String numberRangeQuery(String numberField, double start, double end) {
        // temporarily both inclusive
        String queryTemplate = "%s:[%d TO %d]";
        return String.format(queryTemplate, numberField, start, end);
    }
}
