package com.hna.es.api;

import com.hna.es.bean.IndexTypeProperties;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ESIndexTest {

//    @Test
    public void mapping() {
//        ESIndex.getFileds("test7-apache-uplog");
//        ESIndex.getFileds("yun-1234-928bb12e-cfe0-4de8-84a5-8e0cb08eb77c-container-container-2016-12-09");
        List<IndexTypeProperties> type_fileds = ESIndex.getFileds("test-777");
        for (IndexTypeProperties ip : type_fileds){
            System.out.println(ip.getType()+":\n");
            for (Map.Entry<String, Map<String, Object>> filed : ip.getProperties().entrySet()){
                Map<String, Object> filedMapMap = filed.getValue();
                for (Map.Entry<String, Object> _filed : filedMapMap.entrySet()){
                    System.out.println(filed.getKey()+":"+_filed.getKey()+":"+_filed.getValue().toString());
                    System.out.println("----------------------------");
                }
            }
        }
    }

//    @Test
    public void creteIndexTimeTest() {
        long start = 0l;
        long end = 0l;
        for (int i=200;i<1000;i++){
            start = new Date().getTime();
            boolean index = ESIndex.createIndex("yun-1234-928bb12e-cfe0-4de8-84a5-8e0cb08eb77c-container-container-2016-12-09"+i);
            end = new Date().getTime();
            System.out.print(i+":"+index + ",useTime:" + (end - start)/1000.0 + "s \t");
            if (i%7 == 0) System.out.println();
        }


//        boolean index = ESIndex.createIndex("yun-1234-928bb12e-cfe0-4de8-84a5-8e0cb08eb77c-container-container-2016-12-09");

//        boolean index = ESIndex.createIndex("es-test-test");
//        System.out.println(ZonedDateTime.now().toLocalDateTime());
//        System.out.println("0"+4);
    }

//    @Test
    public void writeES() throws Exception {
        Random random = new Random();
        int [] codes = new int[]{200,200,200,200,200,304,404};

        for (int i=0;i<1000000;i++){
            int h = random.nextInt(24);
            int i1 = random.nextInt(60);
            int i2 = random.nextInt(60);
            String hh="",mm="",mi = "";
            if (h<10) hh = "0" + h; else hh = "" + h;
            if (i1<10) mm = "0" + i1;else mm = "" + i1;
            if (i2<10) mi = "0" + i2; else mi = "" + i2;
            ZonedDateTime now = ZonedDateTime.now();
            String json = "{" +
                    "    \"type\":\"nginx\"," +
                    "    \"@timestamp\":\""+ now.toLocalDateTime()+""+now.getOffset() + "\"," +
                    "    \"timestamp\":\""+ now.toLocalDateTime()+""+now.getOffset() + "\"," +
                    "    \"data\":{" +
                    "        \"container_uuid\":\"abc123\"," +
                    "        \"environment_id\":\"def456\"," +
                    "        \"app_file\":\"nginx_file1\"," +
                    "        \"arr\": [{\"key\":\"haha\",\"value\":123},{\"key\":\"haha2\",\"value\":1234}]," +
                    "        \"log_info\":{" +
                    "            \"remote\":\"172.16.6."+random.nextInt(200)+"\"," +
                    "            \"host\":\"host_"+random.nextInt(5)+"\"," +
                    "            \"user\":\"-\"," +
                    "            \"method\":\"GET\"," +
                    "            \"path\":\"/\"," +
                    "            \"code\":"+codes[random.nextInt(7)]+"," +
                    "            \"size\":"+random.nextInt(1000)+"," +
                    "            \"referer\":\"-\"," +
                    "            \"arr2\": [\"haha\",\"haha2\",\"haha3\"]," +
                    "            \"info2\":{" +
                    "                   \"age\":"+random.nextInt(100)  +
                    "            }," +
                    "            \"agent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\"" +
                    "        }" +
                    "    }" +
                    "}";
            ESDoc.addJsonDocument("log-6b9eime4w5-iiv33-"+now.toLocalDate(),"log",json);
            System.out.println("line:"+i);

            Thread.sleep(5000);
        }

    }

//    @Test
    public void testES() throws Exception {
        boolean exists = ESIndex.isExists("log-6b9eime4w5-3imo3");
        System.out.println(exists);

    }

//    @Test
    public void getIndexByte() throws Exception {
//        ESIndex.getIndexByte();
        Map<String, Long> allIndexCreationDate = ESIndex.getAllIndexCreationDate("ip-");
        System.out.println(allIndexCreationDate);
    }
}
