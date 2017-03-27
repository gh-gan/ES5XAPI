package com.hna.es.service;

import com.hna.es.api.ESAgg;
import com.hna.es.entity.*;
import com.hna.es.util.QueryStringBuilders;
import com.hna.es.util.SortUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.Map;
import java.util.Set;

public class ChartsDataService {
    /**
     * @param indexes      索引名称
     * @param chartContext 图表数据属性上下文，{@link ChartContext}
     * @param range        统计数据时间范围，milliseconds
     * @param interval     直方图可选时间片段间隔
     * @return Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public Map<String, Object> histogramAggregate(String[] indexes, ChartContext chartContext, TimeRange range, DateHistogramInterval interval) {
        String timeRangeQuery = QueryStringBuilders.timeRangeQuery(chartContext.getxSeries(),
                range.getStarting().getTime(), range.getEnding().getTime());
        Map<String, Object> result = ESAgg.dateHistogramAggregate(indexes, timeRangeQuery, chartContext.getxSeries(),
                interval, chartContext.getySeries());
        return result;
    }

    /**
     * @param indexes      索引名称
     * @param chartContext 图表数据属性上下文，{@link ChartContext}
     * @param range        统计数据时间范围，milliseconds
     * @param intervals    各个统计时间片段
     * @return Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public Map<String, Object> timeSlicesAggregate(String[] indexes, ChartContext chartContext, TimeRange range, TimeRange[] intervals) {
        String timeRangeQuery = QueryStringBuilders.timeRangeQuery(chartContext.getxSeries(),
                range.getStarting().getTime(), range.getEnding().getTime());
        Map<String, Object> result = ESAgg.timeSlicesAggregate(indexes, timeRangeQuery, chartContext.getxSeries(),
                intervals, chartContext.getySeries());
        return result;
    }

    /**
     * @param indexes        索引名称
     * @param chartContext   图表数据属性上下文，{@link ChartContext}
     * @param range          统计数据时间范围，milliseconds
     * @param order          字段排序，DESC或ESC
     * @param limit          限制返回结果数量
     * @param timestampField 检索过滤时间字段
     * @return Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public Map<String, Object> seriesAggregate(String[] indexes, ChartContext chartContext, TimeRange range,
                                               Order order, int limit, String timestampField) {
        String timeRangeQuery = QueryStringBuilders.timeRangeQuery(timestampField,
                range.getStarting().getTime(), range.getEnding().getTime());
        Map<String, Object> result = ESAgg.fieldSeriesAggregate(indexes, timeRangeQuery, chartContext.getxSeries(),
                order, limit, chartContext.getySeries());
        if(getOrderByField(order, chartContext) != null)
            result = SortUtils.SortOnMapStringToList(result, getOrderByField(order, chartContext),
                    order.getOrdering().equals(Order.Ordering.ESC));
        return result;
    }

    /**
     * @param indexes        索引名称
     * @param chartContext   图表数据属性上下文，{@link ChartContext}
     * @param range          统计数据时间范围，milliseconds
     * @param slices         横坐标区间片段
     * @param timestampField 检索过滤时间字段
     * @return Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public Map<String, Object> fieldSlicesAggregate(String[] indexes, ChartContext chartContext, TimeRange range,
                                                    Range[] slices, String timestampField) {
        String timeRangeQuery = QueryStringBuilders.timeRangeQuery(timestampField,
                range.getStarting().getTime(), range.getEnding().getTime());
        Map<String, Object> result = ESAgg.fieldSlicesAggregate(indexes, timeRangeQuery, chartContext.getxSeries(), slices,
                chartContext.getySeries());
        return result;
    }

    private String getOrderByField(Order order, ChartContext chartContext) {
        Set<SeriesContext> ySeries = chartContext.getySeries();
        for (SeriesContext context : ySeries)
            if (context.getField().equals(order.getOrderBy()) && order.getAggregationType().equals(context.getAggregationType()))
                return context.getField() + "_" + context.getAggregationType().getComment();
        return null;
    }
}
