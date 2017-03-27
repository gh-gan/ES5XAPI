package com.hna.es.entity;

/**
 * 表示一个图表数据中的Y轴的一个系列的主要属性。
 * <pre>
 * 这个类包含3个字段：
 *      name：这个系列的名称，比如：cpu使用率
 *      field： 这个系列对应的取值来源字段
 *      aggregationType： 数值的聚合类型
 * </pre>
 */
public class SeriesContext {
    private String name;
    private String field;
    private AggregationType aggregationType;

    public SeriesContext(String name, String field, AggregationType aggregationType) {
        this.name = name;
        this.field = field;
        this.aggregationType = aggregationType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    @Override
    public int hashCode() {
        return this.getName().hashCode() + this.getField().hashCode() + this.getAggregationType().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SeriesContext
                && ((SeriesContext)obj).getName().equals(this.getName())
                && ((SeriesContext)obj).getField().equals(this.getField())
                && ((SeriesContext)obj).getAggregationType().equals(this.getAggregationType());
    }
}
