package com.hna.es.api;

import com.hna.es.bean.PagingResult;
import com.hna.es.util.ES;
import com.hna.es.util.ESClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

/**
 * Created by GH-GAN on 2016/8/10.
 */
public class ESSearch extends ES{

    /**
     * 通过索引及查询语句查询结果，默认返回10条，如想设置最大返回行数请使用queryStringLimit方法
     *
     * <pre> indexName/queryString 写法 examples:
     *             indexName : 某个索引：     {"hnagroup-2016.07.16","hnagroup-2016.07.17"}
     *                         多个索引匹配： {"hnagroup-*"}
     *            queryString :  字段查询：  "user:gh"
     *                           字段匹配：  "user:gh*"  / "user:gh?"
     *                           多个条件：  "user:gh OR "message:gh?"" / "user:gh* AND "message:gh""
     *                           范围查询：  "num:[3 TO 8]"   /   "num:{3 TO 8}"  /  "user:gh AND num:[3 TO 8]"
     *                           注意：num：必须为数字或时间类型(时间必需是iso8601格式[2016-11-20T09:10:10.134Z]) </pre>
     *
     * @param indexName 某个索引或多个索引
     * @param queryString  查询语句
     * @throws Exception    异常类型
     * @return  一个文档对应一个Map
     */
    public static List<Map<String,Object>> queryString(String [] indexName, String queryString) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);

        SearchResponse scrollResp = client.prepareSearch(indexName)
//                .addSort("data.stats.timestamp", SortOrder.ASC)
                .setQuery(qb)
                .execute().actionGet();

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (SearchHit hit : scrollResp.getHits().getHits()) {
            list.add(hit.getSource());
        }
        return list;
    }

    public static SearchHit[] queryString2(String [] indexName, String queryString) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb)
                .execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    /**
     * 获取指定字段或排除指定字段
     * @param indexName
     * @param queryString
     * @param includesfields
     * @param excludesfields
     * @return
     * @throws Exception
     */
    public static SearchHit[] queryString2(String [] indexName, String queryString, @Nullable String[] includesfields, @Nullable String[] excludesfields) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb).setFetchSource(includesfields,excludesfields)
                .setSize(0).execute().actionGet();
        return scrollResp.getHits().getHits();
    }

    /**
     * 获取指定字段或排除指定字段
     * @param indexName
     * @param docType
     * @param queryString
     * @param includesfields
     * @param excludesfields
     * @return
     * @throws Exception
     */
    public static SearchHit[] queryString2(String [] indexName,String [] docType,String queryString, @Nullable String[] includesfields, @Nullable String[] excludesfields) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(docType)
                .setQuery(qb).setFetchSource(includesfields,excludesfields).setSize(100000000)
                .execute().actionGet();
        return scrollResp.getHits().getHits();
    }

    public static SearchHit[] queryString2(String [] indexName, String queryString,int startIndex,int size) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
//                .addSort("data.log_info.log_time", SortOrder.ASC)
                .setFrom(startIndex)
                .setSize(size)
                .setQuery(qb)
                .setSearchType(SearchType.DEFAULT)
                .execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    /**
     * 分页查询
     * @param indexName
     * @param queryString
     * @param startIndex
     * @param size
     * @return  返回分页结果
     * @throws Exception
     */
    public static PagingResult queryStringPaging(String [] indexName, String queryString, int startIndex, int size) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setFrom(startIndex)
                .setSize(size)
                .setQuery(qb)
                .setSearchType(SearchType.DEFAULT)
                .execute().actionGet();

        return new PagingResult(scrollResp.getHits().getTotalHits(),scrollResp.getHits().getHits());
    }

    /**
     * 分页查询
     * @param indexName
     * @param type
     * @param queryString
     * @param startIndex
     * @param size
     * @return  返回分页结果
     * @throws Exception
     */
    public static PagingResult queryStringPaging(String [] indexName,String [] type, String queryString, int startIndex, int size) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(type)
                .setFrom(startIndex)
                .setSize(size)
                .setQuery(qb)
                .setSearchType(SearchType.DEFAULT)
                .execute().actionGet();

        return new PagingResult(scrollResp.getHits().getTotalHits(),scrollResp.getHits().getHits());
    }

    /**
     * 分页查询, 可指定字段排序
     * @param indexName
     * @param queryString
     * @param startIndex
     * @param size
     * @param filed       需要排序的字段
     * @param sortOrder   排序方式  SortOrder.ASC  /  SortOrder.DESC
     * @return  返回分页结果
     * @throws Exception
     */
    public static PagingResult queryStringPaging(String [] indexName, String queryString, int startIndex, int size,String filed,SortOrder sortOrder) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .addSort(filed, sortOrder)
                .setFrom(startIndex)
                .setSize(size)
                .setQuery(qb)
                .setSearchType(SearchType.DEFAULT)
                .execute().actionGet();

        return new PagingResult(scrollResp.getHits().getTotalHits(),scrollResp.getHits().getHits());
    }

    /**
     * 分页查询, 可指定字段排序
     * @param indexName
     * @param type
     * @param queryString
     * @param startIndex
     * @param size
     * @param filed       需要排序的字段
     * @param sortOrder   排序方式  SortOrder.ASC  /  SortOrder.DESC
     * @return  返回分页结果
     * @throws Exception
     */
    public static PagingResult queryStringPaging(String [] indexName,String [] type, String queryString, int startIndex, int size,String filed,SortOrder sortOrder) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(type)
                .addSort(filed, sortOrder)
                .setFrom(startIndex)
                .setSize(size)
                .setQuery(qb)
                .setSearchType(SearchType.DEFAULT)
                .execute().actionGet();

        return new PagingResult(scrollResp.getHits().getTotalHits(),scrollResp.getHits().getHits());
    }

    /**
     *  命中总数
     * @param indexName
     * @param queryString
     * @return
     * @throws Exception
     */
    public static long queryStringCount(String [] indexName, String queryString) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(qb)
                .execute().actionGet();
        return scrollResp.getHits().getTotalHits();
    }

    public static long queryStringCount2(String [] indexName, String queryString,QueryBuilder rangeQuery) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        QueryBuilder bq = boolQuery()
                .must(qb)
                .filter(rangeQuery);
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(bq)
                .execute().actionGet();
        return scrollResp.getHits().getTotalHits();
    }

    /*
     * // 时间范围
     * QueryBuilder qb2 = rangeQuery("data.log_info.log_time")    // 多级字段
     *           .from("2016-11-20T09:10:10.134Z")   //开始时间
     *           .to("2016-11-20T20:10:10.134Z");    //结束时间
     */
    public static SearchHit[] queryStringAndRange(String [] indexName, String queryString,QueryBuilder rangeQuery) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        QueryBuilder bq = boolQuery()
                .must(qb)
                .filter(rangeQuery);

        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(bq)
                .execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    public static SearchHit[] queryStringAndRange(String [] indexName,String queryString,QueryBuilder rangeQuery,int startIndex,int size) throws Exception {
        QueryBuilder qb = queryStringQuery(queryString);
        QueryBuilder bq = boolQuery()
                .must(qb)
                .filter(rangeQuery);

        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setQuery(bq)
                .setFrom(startIndex)
                .setSize(size)
                .execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    /**
     * 通过索引及查询语句查询结果
     *
     * @param indexName 某个索引或多个索引
     * @param queryString  查询语句
     * @param size   返回结果数量
     * @return  一个文档对应一个Map
     */
    public static List<Map<String,Object>> queryStringLimit(String [] indexName, String queryString,int size){
        QueryBuilder qb = queryStringQuery(queryString);

        SearchResponse scrollResp = client.prepareSearch(indexName)
//                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setQuery(qb)
                .setSize(size).execute().actionGet();  // 不设置 size es 默认返回10条

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (SearchHit hit : scrollResp.getHits().getHits()) {
            list.add(hit.getSource());
        }

        return list;
    }


    public static SearchHit[] queryStringLimit2(String [] indexName, String queryString,int size){
        QueryBuilder qb = queryStringQuery(queryString);

        SearchResponse scrollResp = client.prepareSearch(indexName)
//                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setQuery(qb)
                .setSize(size).execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    public static SearchHit[] queryStringLimit2(String [] indexName,String [] type, String queryString,int size){
        QueryBuilder qb = queryStringQuery(queryString);

        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(type)
                .setQuery(qb)
                .setSize(size).execute().actionGet();

        return scrollResp.getHits().getHits();
    }

    /**
     * 通过索引、查询语句、文档类型查询结果
     * @param indexName 某个索引或多个索引
     * @param docType   文档类型
     * @param queryString  查询语句
     * @param size   返回结果数量
     * @return  一个文档对应一个Map
     */
    public static List<Map<String,Object>> queryStringLimit(String [] indexName,String [] docType,String queryString,int size){
        QueryBuilder qb = queryStringQuery(queryString);

        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setTypes(docType)
                .setQuery(qb)
                .setSize(size).execute().actionGet();

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (SearchHit hit : scrollResp.getHits().getHits()) {
            list.add(hit.getSource());
        }

        return list;
    }

    /**
     * 通过索引名获取所有文档类型
     * @param indexName 索引名
     * @return
     */
    public static String[] getTypes(String indexName) {
        Set<String> types = new TreeSet<String>();
        try {
            SearchResponse scrollResp = client.prepareSearch(indexName).execute().actionGet();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                types.add(hit.getType());
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
        return types.toArray(new String[types.size()]);
    }


}
