package com.hna.es.service;

import com.hna.es.api.ESAgg;
import com.hna.es.entity.AggregationType;
import com.hna.es.entity.Operator;
import com.hna.es.entity.Rule;
import com.hna.es.entity.TimeRange;
import com.hna.es.util.QueryStringBuilders;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

public class AlertService {

    /**
     * 计算给定时间范围内指定索引指定字段的出现次数是否满足触发规则。
     * @param indexes   索引范围，列表
     * @param field     统计字段
     * @param range     统计时间限制范围
     * @param timestampField     数据时间戳字段名称
     * @param rule      触发规则
     * @return Map&lt;String, Object&gt;，包括聚合类型{@link AggregationType}，统计值：double，评估是否触发的bool值：true则生效，false则不生效。
     */
    public Map<String, Object> countFieldOccurrenceInTimeRange(String[] indexes, String field, TimeRange range,
                                                               String timestampField, Rule rule) {
        String rangeQuery = QueryStringBuilders.timeRangeQuery(timestampField, range.getStarting().getTime(), range.getEnding().getTime());
        double aggregateResult = ESAgg.aggregate(indexes, rangeQuery, field, rule.getType());
        return getAggregatedResultMap(rule.getType(), aggregateResult, evaluateRule(rule, aggregateResult));
    }

    /**
     * 计算给定时间范围内指定索引指定字段的出现次数是否满足触发规则。
     *
     * @param indexes       索引范围，列表
     * @param field         统计字段
     * @param start         统计时间限制起始，纪元时间，毫秒数（13位）
     * @param end           统计时间限制截止，纪元时间，毫秒数（13位）
     * @param timestampField     数据时间戳字段名称
     * @param type          聚合类型，见{@link AggregationType}
     * @param operator      表达式算子，见{@link Operator}
     * @param threshold     规则的阈值，double类型
     * @return Map&lt;String, Object&gt;，包括聚合类型{@link AggregationType}，统计值：double，评估是否触发的bool值：true则生效，false则不生效。
     */
    public Map<String, Object> countFieldOccurrenceInTimeRange(String[] indexes, String field, long start, long end,
                                                               String timestampField, AggregationType type,
                                                               Operator operator, double threshold) {
        TimeRange range = new TimeRange(new Date(start), new Date(end));
        Rule rule = new Rule(type, operator, threshold);
        return countFieldOccurrenceInTimeRange(indexes, field, range, timestampField, rule);
    }

    /**
     * 计算指定索引在给定时间下的出现次数是否满足给定规则。
     *
     * @param indexes   索引范围，列表
     * @param range     统计时间限制范围
     * @param timestampField     数据时间戳字段名称
     * @param rule      触发规则
     * @return Map&lt;String, Object&gt;，包括聚合类型{@link AggregationType}，统计值：double，评估是否触发的bool值：true则生效，false则不生效。
     */
    public Map<String, Object> countEventsInTimeRange(String[] indexes, TimeRange range, String timestampField, Rule rule) {
        String query = QueryStringBuilders.timeRangeQuery(timestampField, range.getStarting().getTime(), range.getEnding().getTime());
        long count = ESAgg.count(indexes, query);
        return getAggregatedResultMap(rule.getType(), count, evaluateRule(rule, count));
    }

    /**
     * 计算指定索引在给定时间下的出现次数是否满足给定规则。
     *
     * @param indexes   索引范围，列表
     * @param start     统计时间限制起始，纪元时间，毫秒数（13位）
     * @param end       统计时间限制截止，纪元时间，毫秒数（13位）
     * @param timestampField     数据时间戳字段名称
     * @param type      聚合类型，见{@link AggregationType}
     * @param operator  表达式算子，见{@link Operator}
     * @param threshold 规则的阈值，double类型
     * @return Map&lt;String, Object&gt;，包括聚合类型{@link AggregationType}，统计值：double，评估是否触发的bool值：true则生效，false则不生效。
     */
    public Map<String, Object> countEventsInTimeRange(String[] indexes, long start, long end, String timestampField, AggregationType type,
                                       Operator operator, double threshold) {
        TimeRange range = new TimeRange(new Date(start), new Date(end));
        return countEventsInTimeRange(indexes, range, timestampField, new Rule(type, operator, threshold));
    }

    /**
     * 应用规则{@link Rule}到一个实际值，判别表达式为true或false。
     * @param rule              触发规则对象
     * @param aggregateResult   实际值。
     * @return bool值，true则生效，false则不生效。
     */
    private boolean evaluateRule(Rule rule, double aggregateResult) {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        Object result = "";
        try {
            result = engine.eval(String.valueOf(aggregateResult) +
                    rule.getOperator().getSymbol() + rule.getThreshold());
        } catch (ScriptException e) {
            e.printStackTrace();
        }
        return (boolean) result;
    }

    private Map<String, Object> getAggregatedResultMap(AggregationType type, double statValue, boolean evaluateResult) {
        Map<String, Object> result = new Hashtable<>();
        result.put("aggregationType", type);
        result.put("statValue", statValue);
        result.put("evaluatedResult", evaluateResult);
        return result;
    }
}
