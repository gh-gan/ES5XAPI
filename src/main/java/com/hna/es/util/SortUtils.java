package com.hna.es.util;

import java.util.*;

public class SortUtils {
    public static Map<String, Object> SortOnMapStringToList(Map<String, Object> aggregrateResult, String field, boolean ascending) {
        List<Number> xSeries = (ArrayList<Number>)aggregrateResult.get("xSeries");
        Object[] _xSeries = xSeries.toArray();
        Arrays.sort(_xSeries);
        int[] indexes = new int[xSeries.size()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = xSeries.indexOf(_xSeries[i]);
        }
        if (! ascending) indexes = reverse(indexes);
        Map<String, Object> result = new HashMap<>();
        for (String key : aggregrateResult.keySet())
            result.put(key, new ArrayList<Number>());
        for (int index : indexes)
            for (String key : aggregrateResult.keySet()) {
                ((List<Number>) result.get(key)).add(((List<Number>) aggregrateResult.get(key)).get(index));
            }
        return result;
    }

    private static int[] reverse(int[] array) {
        int[] numbers = Arrays.copyOf(array, array.length);
        for (int i = 0; i < numbers.length / 2; i++) {
            int tmp = numbers[i];
            numbers[i] = numbers[numbers.length-i-1];
            numbers[numbers.length-i-1] = tmp;
        }
        return numbers;
    }
}
