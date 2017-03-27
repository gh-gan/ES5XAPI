package com.hna.es.service;

import com.hna.es.entity.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ChartsDataServiceTest {
    private ChartsDataService chartsDataService;

    @Before
    public void setup() {
        chartsDataService = new ChartsDataService();
    }

    @Test
    public void shouldReturnHistogramDataFieldCpuUsageInterval10Sec() {
        String fieldX = "data.timestamp";
        String fieldY = "data.age";
        String[] indexes = {"testuniqu"};
        // start time: Sat, 17 Dec 2016 12:30:00 GMT,
        // end time: Sat, 17 Dec 2016 12:30:00 GMT
        TimeRange range = new TimeRange(new Date(1481976900000L), new Date(1487041809188L));
        HashSet<SeriesContext> seriesContexts = new HashSet<>();
        seriesContexts.add(new SeriesContext("y1", fieldY, AggregationType.COUNT));
        seriesContexts.add(new SeriesContext("y2", fieldY, AggregationType.SUM));
        ChartContext chartContext = new ChartContext("chart", fieldX, seriesContexts);
        Map<String, Object> result = chartsDataService.histogramAggregate(indexes, chartContext, range, DateHistogramInterval.MINUTE);
//        assertEquals("dateHistogram should return 3 k/vs data...", 3, result.size());
//        assertEquals("dateHistogram should return 15 count data...", 15, ((List)result.get(fieldY + "_count")).size());
//        assertEquals("dateHistogram should return 15 min data...", 15, ((List)result.get(fieldY + "_min")).size());
        System.out.println(result.size());
        System.out.println(((List)result.get(fieldY + "_count")).toString());
        System.out.println(((List)result.get(fieldY + "_uniquecount")).toString());
    }

    @Test
    public void shouldGetTimeSlicesCpuUsageForTimeSlices() {
        String fieldX = "data.stats.timestamp";
        String fieldY = "data.stats.container_cpu_usage_seconds_total";
        String[] indexes = {"yun-10--container-container-2016-12-17"};
        // start time: Sat, 17 Dec 2016 12:30:00 GMT,
        // end time: Sat, 17 Dec 2016 12:30:00 GMT
        TimeRange range = new TimeRange(new Date(1481963405000L), new Date(1481992205000L));
        TimeRange[] intervals = {
                new TimeRange(new Date(1481991005000L), new Date(1481992205000L)),
                new TimeRange(new Date(1481967005000L), new Date(1481968805000L))
        };
        HashSet<SeriesContext> seriesContexts = new HashSet<>();
        seriesContexts.add(new SeriesContext("y1", fieldY, AggregationType.COUNT));
        seriesContexts.add(new SeriesContext("y2", fieldY, AggregationType.MAX));
        ChartContext chartContext = new ChartContext("chart", fieldX, seriesContexts);
        Map<String, Object> result = chartsDataService.timeSlicesAggregate(indexes, chartContext, range, intervals);
        assertEquals("time slices should return 3 k/vs data...", 3, result.size());
        assertEquals("first slice should have 1181 docs", 2, ((List)result.get(fieldY + "_count")).size());
        assertEquals("first slice should have 1181 docs", 2, ((List)result.get(fieldY + "_max")).size());
    }

    @Test
    public void shouldGetSeriesCpuUsageData() {
        String fieldX = "data.stats.container_network_receive_packets_total";
        String fieldY = "data.stats.container_cpu_usage_seconds_total";
        String[] indexes = {"yun-10--container-container-2016-12-17"};
        // start time: Sat, 17 Dec 2016 12:30:00 GMT,
        // end time: Sat, 17 Dec 2016 12:30:00 GMT
        TimeRange range = new TimeRange(new Date(1481963405000L), new Date(1481992205000L));
        HashSet<SeriesContext> seriesContexts = new HashSet<>();
        seriesContexts.add(new SeriesContext("y1", fieldY, AggregationType.AVG));
        seriesContexts.add(new SeriesContext("y2", fieldY, AggregationType.MIN));
        ChartContext chartContext = new ChartContext("chart", fieldX, seriesContexts);
        Order order = new Order(fieldY, Order.Ordering.DESC);
        order.setAggregationType(AggregationType.AVG);
        Map<String, Object> result = chartsDataService.seriesAggregate(indexes, chartContext, range,
                order, 10, "data.stats.timestamp");
        assertEquals("series should return 3 k/vs data...", 3, result.size());
        assertEquals("series should return 10 avg data...", 10, ((List)result.get(fieldY + "_avg")).size());
        assertEquals("series should return 10 min data...", 10, ((List)result.get(fieldY + "_min")).size());
    }

    @Test
    public void shouldGetFieldSlicesSeriesMemUseWithCpuUsageData() {
        String fieldX = "data.stats.container_memory_usage_bytes";
        String fieldY = "data.stats.container_cpu_usage_seconds_total";
        String[] indexes = {"yun-10--container-container-2016-12-17"};
        // start time: Sat, 17 Dec 2016 12:30:00 GMT,
        // end time: Sat, 17 Dec 2016 12:30:00 GMT
        TimeRange range = new TimeRange(new Date(1481963405000L), new Date(1481992205000L));
        HashSet<SeriesContext> seriesContexts = new HashSet<>();
        seriesContexts.add(new SeriesContext("y1", fieldY, AggregationType.AVG));
        seriesContexts.add(new SeriesContext("y2", fieldY, AggregationType.COUNT));
        ChartContext chartContext = new ChartContext("chart", fieldX, seriesContexts);
        Range[] slices = new Range[]{new Range(0, 30000000L), new Range(30000000L, 38000000L), new Range(38000000L, 40000000L)};
        Map<String, Object> result = chartsDataService.fieldSlicesAggregate(indexes, chartContext, range, slices,"data.stats.timestamp");
        assertEquals("field slices should return 3 k/vs data...", 3, result.size());
        assertEquals("series should return 3 avg data ...", 3, ((List)result.get(fieldY + "_avg")).size());
        assertEquals("series should return 3 avg data ...", 3, ((List)result.get(fieldY + "_count")).size());
    }
}