package com.hna.es.entity;

import java.util.Set;

/**
 * 表示图标数据属性的上下文。包括以下字段：
 * <pre>
 *    1. chartName，字符串；
 *    2. xSeries，字符串；
 *    3. ySeries，y轴系列值，Set类型，参考{@link SeriesContext}；
 * </pre>
 */
public class ChartContext {
    private String chartName;
    private String xSeries;
    private Set<SeriesContext> ySeries;

    public ChartContext(String chartName, String xSeries, Set<SeriesContext> ySeries) {
        this.chartName = chartName;
        this.xSeries = xSeries;
        this.ySeries = ySeries;
    }

    public String getChartName() {
        return chartName;
    }

    public void setChartName(String chartName) {
        this.chartName = chartName;
    }

    public String getxSeries() {
        return xSeries;
    }

    public void setxSeries(String xSeries) {
        this.xSeries = xSeries;
    }

    public Set<SeriesContext> getySeries() {
        return ySeries;
    }

    public void setySeries(Set<SeriesContext> ySeries) {
        this.ySeries = ySeries;
    }
}
