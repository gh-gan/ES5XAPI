package com.hna.es.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hna.es.util.ES;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TypeQueryBuilder;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by GH-GAN on 2016/8/8.
 */
public class ESDoc extends ES{
    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * 添加一个文档
     * @param index  文档所在索引
     * @param type 文档类型
     * @param data 键值对数据
     * @return 该文档在ES中的ID值
     * @throws Exception 数据转换异常
     */
    public static String addDocument(String index,String type,Map<String, Object> data) throws Exception{
        byte[] json = mapper.writeValueAsBytes(data);
        IndexResponse response = client.prepareIndex(index, type).setSource(json).get();
        return response.getId();
    }

    /**
     * 添加一个文档
     * @param index  文档所在索引
     * @param type 文档类型
     * @param json json数据
     * @return 该文档在ES中的ID值
     * @throws Exception 数据转换异常
     */
    public static String addJsonDocument(String index,String type,String json) throws Exception{
        IndexResponse response = client.prepareIndex(index, type).setSource(json).get();
        return response.getId();
    }


    /**
     * 添加多个文档
     * @param index  文档所在索引
     * @param type 文档类型
     * @param jsons 批量数据
     * @throws Exception 异常类型
     */
    public static void addDocuments(String index,String type,ArrayList<String> jsons) throws Exception{
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String json : jsons)
            bulkRequest.add(client.prepareIndex(index, type).setSource(json));
        bulkRequest.execute().actionGet();
    }

    /**
     * 添加多个文档
     * @param index  文档所在索引
     * @param type 文档类型
     * @param jsons 批量数据
     * @param size  多少条提交一次
     * @throws Exception    抛出异常类型
     */
    public static void addDocuments2(String index,String type,List<String> jsons,int size) throws Exception{
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 0; i < jsons.size(); i++) {
            bulkRequest.add(client.prepareIndex(index, type).setSource(jsons.get(i)));
            // 每 size 条提交一次
            if (i % size == 0) {
                bulkRequest.get();
            }
        }
        bulkRequest.get();
    }

    /**
     * 更新某个文档
     * @param index 文档所在索引
     * @param type 文档类型
     * @param id 要更新文档的ID值
     * @param data 要更新的键值对
     * @return 是否更新成功
     * @throws Exception 数据转换异常
     */
    public static boolean updateDocument(String index,String type,String id,Map<String, Object> data) throws Exception{
        if (isExists(index, type, id)) {
            byte[] json = mapper.writeValueAsBytes(data);
            UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(json).get();
            return true;
        }
        return false;
    }

    /**
     * 获取一个文档
     * @param index 索引
     * @param type 类型
     * @param id 文档在ES中的ID值
     * @return 不存在返回Null
     */
    public static Map<String, Object> getDocument(String index,String type,String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        if (response.isExists())
            return response.getSource();
        return null;
    }

    /**
     * 删除一个文档
     * @param index 索引
     * @param type 类型
     * @param id 文档在ES中的ID值
     * @return 是否删除成功
     */
    public static int deleteDocument(String index,String type,String id){
        DeleteResponse response = client.prepareDelete(index, type, id).get();
        return response.status().getStatus();
    }

    /**
     * 删除index下的某个type下的所有数据（type未删除）
     * @param index
     * @param type
     * @return 删除的记录数
     */
    public static long deleteType(String[] index,String type){
        BulkIndexByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.typeQuery(type))
                        .source(index)
                        .get();
        long deleted = response.getDeleted();
        return deleted;
    }

    private static boolean isExists(String index,String type,String id){
        return client.prepareGet(index, type, id).get().isExists();
    }

}
