package com.hna.es.api;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.hna.es.bean.IndexMapping;
import com.hna.es.bean.IndexTypeProperties;
import com.hna.es.util.ES;
import com.hna.es.util.ESConfig;
import com.hna.es.util.HttpUtil;
import com.hna.es.util.UnitChangeUtil;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.core.util.SystemClock;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

/**
 * Created by GH-GAN on 2016/8/8.
 */
public class ESIndex extends ES{
    private static Logger logger = Logger.getLogger(ESIndex.class);

    /**
     * 创建一个索引
     * @param indexName 索引名
     * @return 是否创建成功
     */
    public static boolean createIndex(String indexName) {
        try {
            client.admin().indices().prepareCreate(indexName).get();
        } catch (Exception ex){
            return false;
        }
        return true;
    }

    /**
     * 删除一个索引
     * @param indexName 索引名
     * @return 是否删除成功
     */
    public static boolean deleteIndex(String indexName) {
        try {
            client.admin().indices().prepareDelete(indexName).get();
        } catch (Exception ex){
            return false;
        }
        return true;
    }

    /**
     * 判断index是否存在
     * @param indexName 索引名
     * @return true 表示存在
     */
    public static boolean isExists(String indexName) {
        try {
            IndicesExistsResponse indexs = client.admin().indices().exists(new IndicesExistsRequest(new String[]{indexName})).get();
            return indexs.isExists();
        } catch (Exception ex){
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * 获取所有index
     * @return 字符串数据
     */
    public static String[] getAllIndexName(){
        String[] indices = null;
        try {
            GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().execute().actionGet();
            indices = getIndexResponse.getIndices();
        } catch (Exception ex){
            ex.printStackTrace();
        }
        return indices;
    }

    /**
     * 获取所有index及其创建时间
     * @param indexStartName  只取index名称是indexStartName开头的，* 表示取所有index
     * @return  key:indexName   value:index创建时间
     */
    public static Map<String,Long> getAllIndexCreationDate(String indexStartName) {
        Map<String,Long> indexs = new HashMap<String,Long>();
        try {
            GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().execute().actionGet();
            ImmutableOpenMap<String, Settings> settings = getIndexResponse.getSettings();
            for (ObjectObjectCursor<String, Settings> next : settings) {
                String indexName = next.key;
                if (indexName.startsWith(indexStartName) || "*".equals(indexStartName.trim())){
                    Settings value = next.value;
                    String index_creation_date = value.get("index.creation_date");
                    indexs.put(indexName, Long.parseLong(index_creation_date));
                }
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
        return indexs;
    }

    /**
     * 获取index 文档类型及字段
     * @param indexName 索引名称
     * @return  key:indexName   value:index创建时间
     */
    public static List<IndexTypeProperties> getFileds(String indexName) {
//        Map<String,String> index_type_properties = new HashMap<String,String>();
        List<IndexTypeProperties> type_fileds = new LinkedList<IndexTypeProperties>();
        try {
            GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(new String[]{indexName}).execute().actionGet();
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getMappingsResponse.getMappings();
            ObjectContainer<ImmutableOpenMap<String, MappingMetaData>> values = mappings.values();
            Iterator<ObjectCursor<ImmutableOpenMap<String, MappingMetaData>>> iterator = values.iterator();
            while (iterator.hasNext()){
                ObjectCursor<ImmutableOpenMap<String, MappingMetaData>> next = iterator.next();
                ImmutableOpenMap<String, MappingMetaData> value = next.value;
                ObjectLookupContainer<String> keys = value.keys();
                for (ObjectCursor<String> key : keys){
                    String docType = key.value;
                    MappingMetaData mappingMetaData = value.get(docType);
                    Map<String, Object> sourceAsMap = mappingMetaData.getSourceAsMap();
                    /*for (Map.Entry<String, Object> ob : sourceAsMap.entrySet()){
                        System.out.println(ob.getKey()+" : "+ob.getValue().toString());
                    }*/
//                    String properties = sourceAsMap.get("properties").toString();
                    Map<String, Map<String, Object>> properties = (Map<String, Map<String,Object>>)sourceAsMap.get("properties");
                    /*for (Map.Entry<String, Map<String, Object>> filed : properties.entrySet()){
                        Map<String, Object> filedMapMap = filed.getValue();
                        for (Map.Entry<String, Object> _filed : filedMapMap.entrySet()){
                            System.out.println(filed.getKey()+":"+_filed.getKey()+"--"+_filed.getValue().toString());
                            System.out.println("----------------------------");
                        }
                    }*/
                    IndexTypeProperties itp = new IndexTypeProperties();
                    itp.setType(docType);
                    itp.setProperties(properties);
                    type_fileds.add(itp);
                }
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
//        IndexMapping indexMapping = new IndexMapping();
//        indexMapping.setType_fileds(type_fileds);
        return type_fileds;
    }

    public static String getIndexStoreSize(String indexName) {
        String[] nodes = ESConfig.nodes.split(",");
        List<String> get = HttpUtil.Get("http://" + nodes[0] + ":" + ESConfig.httpPort + "/_cat/indices/" + indexName + "?h=store.size");
        if (get.size() == 0) return "0";
        if (get.size() == 1){ return get.get(0).toUpperCase(); }
        double size = 0.0;
        for(String _size : get){
            size = size + UnitChangeUtil.toMb(_size);
        }
        return  UnitChangeUtil.mbToFitUnit(size);
    }

}
