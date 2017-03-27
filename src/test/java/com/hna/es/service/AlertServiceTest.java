package com.hna.es.service;

import com.hna.es.entity.AggregationType;
import com.hna.es.entity.Operator;
import com.hna.es.entity.Rule;
import com.hna.es.entity.TimeRange;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AlertServiceTest {
    private AlertService alertService;

    @Before
    public void setup() {
        alertService = new AlertService();
    }

    @Test
    public void shouldCountFieldOccurrenceInTimeRange() {
        String[] indexes = {"yun-10--container-container-2016-12-16"};
        String field = "data.stats.container_memory_usage_bytes";
        // start time: Fri, 16 Dec 2016 11:00:00 GMT
        // end time: Fri, 16 Dec 2016 12:00:00 GMT
        TimeRange range = new TimeRange(new Date(1481886000000L), new Date(1481889600000L));
        Rule rule = new Rule(AggregationType.COUNT, Operator.EQ, 2413L);
        Map<String, Object> actual = alertService.countFieldOccurrenceInTimeRange(indexes, field, range, "data.stats.timestamp", rule);
        assertEquals("Count field should return correct value", 2413.0, actual.get("statValue"));
        assertEquals("should return true evaluated", true, actual.get("evaluatedResult"));
    }

    @Test
    public void shouldCountEventsInTimeRange() {
        String[] indexes = {"yun-10--container-container-2016-12-16"};
        // start time: Fri, 16 Dec 2016 11:00:00 GMT
        // end time: Fri, 16 Dec 2016 12:00:00 GMT
        TimeRange range = new TimeRange(new Date(1481886000000L), new Date(1481889600000L));
        Rule rule = new Rule(AggregationType.COUNT, Operator.EQ, 2413L);
        Map<String, Object> actual = alertService.countEventsInTimeRange(indexes, range, "data.stats.timestamp", rule);
        assertEquals("Should return correct docs count value", 2413.0, actual.get("statValue"));
        assertEquals("should return true evaluated", true, actual.get("evaluatedResult"));
    }
}
