package com.hna.es.api;

import com.hna.es.entity.AggregationType;
import com.hna.es.entity.Order;
import com.hna.es.entity.SeriesContext;
import com.hna.es.entity.TimeRange;
import com.hna.es.util.ES;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.terms.InternalMappedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.joda.time.DateTime;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

/**
 * Created by GH-GAN on 2016/8/10.
 */
public class ESAgg extends ES {

    /**
     * 聚类求值
     *
     * @param indexName 索引名
     * @param aggField  要聚类的字段值
     * @param aggType   聚类类型 ： ｛"count"，"sum","avg","min","max"｝
     * @return 返回聚合值
     */
    public static double aggregate(String[] indexName, String aggField, AggregationType aggType) {
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .addAggregation(AggregationBuilders.stats("stats").field(aggField))
                .setSize(0).execute().actionGet();
        return ESAggUtils.getValue(scrollResp, aggType);
    }

    /**
     * 聚类求值
     *
     * @param indexName   索引名
     * @param queryString 通过查询过滤数据
     * @param aggField    要聚类的字段值
     * @param aggType     聚类类型 ： ｛"count"，"sum","avg","min","max"｝
     * @return 返回聚合值
     */
    public static double aggregate(String[] indexName, String queryString, String aggField, AggregationType aggType) {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb)
                .addAggregation(AggregationBuilders.stats("stats").field(aggField))
                .setSize(0).execute().actionGet();
        return ESAggUtils.getValue(scrollResp, aggType);
    }

    /**
     * 聚类求值
     *
     * @param indexName   索引名
     * @param docType     过滤文档类型
     * @param queryString 通过查询过滤数据
     * @param aggField    要聚类的字段值
     * @param aggType     聚类类型 ： ｛"count"，"sum","avg","min","max"｝
     * @return 返回聚合值
     */
    public static double aggregate(String[] indexName, String[] docType, String queryString, String aggField, AggregationType aggType) {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setTypes(docType)
                .setQuery(qb)
                .addAggregation(AggregationBuilders.stats("stats").field(aggField))
                .setSize(0).execute().actionGet();
        return ESAggUtils.getValue(scrollResp, aggType);
    }

    /**
     * @param indexes       索引名称
     * @param queryString   查询语句
     * @param xSeries       X轴时间字段
     * @param interval      X周间隔，{周，月，日，时，分，秒}，{@link DateHistogramInterval}
     * @param ySeries       Y轴时间集合，Set类型，个体是{@link SeriesContext}
     * @return   Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public static Map<String, Object> dateHistogramAggregate(String[] indexes, String queryString, String xSeries,
                                              DateHistogramInterval interval, Set<SeriesContext> ySeries) {
        QueryBuilder query = queryStringQuery(queryString);
        DateHistogramAggregationBuilder aggregationBuilder = AggregationBuilders
                .dateHistogram("histogram")
                .field(xSeries)
                .dateHistogramInterval(interval)
                .order(Histogram.Order.KEY_ASC);
        for (SeriesContext series : ySeries){
            aggregationBuilder.subAggregation(AggregationBuilders.stats(ESAggUtils.formatFieldName(series)).field(series.getField()));
        }
        SearchResponse scrollResp = client.prepareSearch(indexes)
                .setQuery(query)
                .addAggregation(aggregationBuilder)
                .setSize(0).execute().actionGet();
        return ESAggUtils.parseDateHistogramAggregation(scrollResp, ySeries);
    }

    public static long count(String[] indexName, String queryString) {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb)
                .execute().actionGet();
        return scrollResp.getHits().totalHits();
    }


    /**
     * @param indexes           索引名称
     * @param rangeQuery        查询语句
     * @param xSeries           X轴时间字段
     * @param intervals         时间片段列表
     * @param ySeries           Y轴时间集合，Set类型，个体是{@link SeriesContext}
     * @return   Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public static Map<String, Object> timeSlicesAggregate(String[] indexes, String rangeQuery, String xSeries,
                                           TimeRange[] intervals, Set<SeriesContext> ySeries) {
        QueryBuilder query = queryStringQuery(rangeQuery);
        DateRangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders
                .dateRange("date_range").field(xSeries);
        for (SeriesContext series : ySeries)
            rangeAggregationBuilder.subAggregation(AggregationBuilders.stats(ESAggUtils.formatFieldName(series)).field(series.getField()));
        for (TimeRange range : intervals)
            rangeAggregationBuilder.addRange(range.getStarting().getTime(), range.getEnding().getTime());
        SearchResponse scrollResp = client.prepareSearch(indexes)
                .setQuery(query)
                .addAggregation(rangeAggregationBuilder)
                .setSize(0).execute().actionGet();
        return ESAggUtils.parseDateRangeAggregation(scrollResp, ySeries);
    }


    /**
     * @param indexes           索引名称
     * @param rangeQuery        查询语句
     * @param xSeries           X轴时间字段
     * @param order             X轴字段排序方式
     * @param limit             返回结果数量
     * @param ySeries           Y轴时间集合，Set类型，个体是{@link SeriesContext}
     * @return   Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public static Map<String, Object> fieldSeriesAggregate(String[] indexes, String rangeQuery, String xSeries, Order order,
                                            int limit, Set<SeriesContext> ySeries) {
        QueryBuilder query = queryStringQuery(rangeQuery);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                .terms("terms").field(xSeries)
                .order(Terms.Order.term(order.getOrdering().equals(Order.Ordering.ESC)))
                .size(limit);
        for (SeriesContext series : ySeries)
            termsAggregationBuilder.subAggregation(AggregationBuilders
                    .stats(ESAggUtils.formatFieldName(series))
                    .field(series.getField()));
        SearchResponse scrollResp = client.prepareSearch(indexes)
                .setQuery(query)
                .addAggregation(termsAggregationBuilder)
                .setSize(0).execute().actionGet();
        return ESAggUtils.parseSeriesAggregation(scrollResp, ySeries);
    }

    /**
     * @param indexes           索引名称
     * @param rangeQuery        查询语句
     * @param xSeries           X轴时间字段
     * @param slices            区间段数组
     * @param ySeries           Y轴时间集合，Set类型，个体是{@link SeriesContext}
     * @return   Map&lt;String Object&gt;类型，key为坐标, value值为x序列，y1序列,y2序列,...
     */
    public static Map<String, Object> fieldSlicesAggregate(String[] indexes, String rangeQuery, String xSeries,
                                            com.hna.es.entity.Range[] slices, Set<SeriesContext> ySeries) {
        QueryBuilder query = queryStringQuery(rangeQuery);
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders
                .range("range").field(xSeries);
        for (com.hna.es.entity.Range range : slices)
            rangeAggregationBuilder.addRange(range.getStart(), range.getEnd());
        for (SeriesContext series : ySeries)
            rangeAggregationBuilder.subAggregation(AggregationBuilders.stats(ESAggUtils.formatFieldName(series)).field(series.getField()));
        SearchResponse scrollResp = client.prepareSearch(indexes)
                .setQuery(query)
                .addAggregation(rangeAggregationBuilder)
                .setSize(0).execute().actionGet();
        return ESAggUtils.parseRangeAggregation(scrollResp, ySeries);
    }

    /**
     * 字段值去重计数
     * @param indexName
     * @param field
     * @return
     * @throws Exception
     */
    public static Map<Object,Long> fieldUnique(String [] indexName,String field) throws Exception {
        Map<Object,Long> fieldcount = new HashMap<Object,Long>();
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .addAggregation(AggregationBuilders.terms("gh").field(field))
                .setSize(0).execute().actionGet();
        Terms termsgh = scrollResp.getAggregations().get("gh");
        for (Terms.Bucket entry : termsgh.getBuckets()) {
            fieldcount.put(entry.getKey(),entry.getDocCount());
        }
        return fieldcount;
    }

    /**
     * 字段值去重计数
     * @param indexName
     * @param types
     * @param field
     * @return
     * @throws Exception
     */
    public static Map<Object,Long> fieldUnique(String [] indexName,String [] types,String field) throws Exception {
        Map<Object,Long> fieldcount = new HashMap<Object,Long>();
        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(types)
                .addAggregation(AggregationBuilders.terms("gh").field(field))
                .setSize(0).execute().actionGet();
        Terms termsgh = scrollResp.getAggregations().get("gh");
        for (Terms.Bucket entry : termsgh.getBuckets()) {
            fieldcount.put(entry.getKey(),entry.getDocCount());
        }
        return fieldcount;
    }

    /**
     * 字段值去重计数
     * @param indexName
     * @param queryString
     * @param field
     * @return
     * @throws Exception
     */
    public static Map<Object,Long> fieldUnique(String [] indexName,String queryString,String field) throws Exception {
        Map<Object,Long> fieldcount = new HashMap<Object,Long>();
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb)
                .addAggregation(AggregationBuilders.terms("gh").field(field))
                .setSize(0).execute().actionGet();
        Terms termsgh = scrollResp.getAggregations().get("gh");
        for (Terms.Bucket entry : termsgh.getBuckets()) {
            fieldcount.put(entry.getKey(),entry.getDocCount());
        }
        return fieldcount;
    }

    /**
     * 字段值去重计数
     * @param indexName
     * @param types
     * @param queryString
     * @param field
     * @return
     * @throws Exception
     */
    public static Map<Object,Long> fieldUnique(String [] indexName,String [] types,String queryString,String field) throws Exception {
        Map<Object,Long> fieldcount = new HashMap<Object,Long>();
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(types)
                .setQuery(qb)
                .addAggregation(AggregationBuilders.terms("gh").field(field))
                .setSize(0).execute().actionGet();
        Terms termsgh = scrollResp.getAggregations().get("gh");
        for (Terms.Bucket entry : termsgh.getBuckets()) {
            fieldcount.put(entry.getKey(),entry.getDocCount());
        }
        return fieldcount;
    }

    static class ESAggUtils {
        private static double getValue(SearchResponse scrollResp, AggregationType aggType) {
            Stats stats = scrollResp.getAggregations().get("stats");
            return getStats(stats, aggType);
        }

        private static double getStats(Stats stats, AggregationType aggType) {
            switch (aggType) {
                case COUNT:
                    return stats.getCount();
                case SUM:
                    return stats.getSum();
                case AVG:
                    return stats.getAvg();
                case MIN:
                    return stats.getMin();
                case MAX:
                    return stats.getMax();
                default:
                    return 0;
            }
        }

        private static Map<String, Object> parseDateHistogramAggregation(SearchResponse scrollResp, Set<SeriesContext> seriesContexts) {
            List<Histogram.Bucket> buckets = ((InternalDateHistogram) scrollResp.getAggregations().get("histogram")).getBuckets();
            Map<String, Object> result = new HashMap<String, Object>();
            for (SeriesContext series : seriesContexts)
                result.put(formatFieldName(series), new ArrayList<Number>());
            ArrayList<Number> xSeries = new ArrayList<Number>();
            for (Histogram.Bucket bucket : buckets) {
                xSeries.add(((DateTime) bucket.getKey()).getMillis());
                for (SeriesContext series : seriesContexts) {
                    double stat = getStats((InternalStats) bucket.getAggregations().get(formatFieldName(series)), series.getAggregationType());
                    ((List) result.get(formatFieldName(series))).add(stat);
                }
            }
            result.put("xSeries", xSeries);
            return result;
        }

        private static Map<String, Object> parseDateRangeAggregation(SearchResponse scrollResp, Set<SeriesContext> seriesContexts) {
            List<InternalDateRange.Bucket> buckets = ((InternalDateRange) scrollResp.getAggregations().get("date_range")).getBuckets();
            Map<String, Object> result = new HashMap<>();
            for (SeriesContext series : seriesContexts)
                result.put(formatFieldName(series), new ArrayList<Number>());
            ArrayList<Number[]> xSeries = new ArrayList<>();
            for (InternalDateRange.Bucket bucket : buckets) {
                xSeries.add(new Number[]{((DateTime) bucket.getFrom()).getMillis(), ((DateTime) bucket.getTo()).getMillis()});
                for (SeriesContext series : seriesContexts) {
                    double stat = getStats((InternalStats) bucket.getAggregations().get(formatFieldName(series)), series.getAggregationType());
                    ((List) result.get(formatFieldName(series))).add(stat);
                }
            }
            result.put("xSeries", xSeries);
            return result;
        }

        private static Map<String, Object> parseSeriesAggregation(SearchResponse scrollResp, Set<SeriesContext> seriesContexts) {
            List<Terms.Bucket> buckets = ((InternalMappedTerms) scrollResp.getAggregations().get("terms")).getBuckets();
            Map<String, Object> result = new HashMap<>();
            for (SeriesContext series : seriesContexts)
                result.put(formatFieldName(series), new ArrayList<Number>());
            ArrayList<Object> xSeries = new ArrayList<Object>();
            for (Terms.Bucket bucket : buckets) {
                xSeries.add(bucket.getKey());
                for (SeriesContext series : seriesContexts) {
                    double stat = getStats((InternalStats) bucket.getAggregations().get(formatFieldName(series)), series.getAggregationType());
                    ((List) result.get(formatFieldName(series))).add(stat);
                }
            }
            result.put("xSeries", xSeries);
            return result;
        }

        private static Map<String, Object> parseRangeAggregation(SearchResponse scrollResp, Set<SeriesContext> seriesContexts) {
            List<InternalRange.Bucket> buckets = ((InternalRange) scrollResp.getAggregations().get("range")).getBuckets();
            Map<String, Object> result = new HashMap<String, Object>();
            for (SeriesContext series : seriesContexts)
                result.put(formatFieldName(series), new ArrayList<Number>());
            ArrayList<Number[]> xSeries = new ArrayList<>();
            for (InternalRange.Bucket bucket : buckets) {
                xSeries.add(new Number[]{(Number) bucket.getFrom(), (Number) bucket.getTo()});
                for (SeriesContext series : seriesContexts) {
                    double stat = getStats((InternalStats) bucket.getAggregations().get(formatFieldName(series)), series.getAggregationType());
                    ((List) result.get(formatFieldName(series))).add(stat);
                }
            }
            result.put("xSeries", xSeries);
            return result;
        }

        private static String formatFieldName(SeriesContext series) {
            return series.getField() + "_" + series.getAggregationType().getComment();
        }
    }
}
