package com.hna.es.api;

import org.elasticsearch.search.SearchHit;
import org.junit.Test;

/**
 * Created by GH-GAN on 2017/1/19.
 */
public class ESSearchTest {
//    @Test
    public void testGetField() throws Exception{
//        SearchHit[] searchHits = ESSearch.queryString2(new String[]{"yun-5-4a5cae51-fdd3-4e01-87c9-de6f4dc53254-applogfile-nginx-2016-12-22"}, "*", new String[]{"data.app_file"},null);
        ESAgg.fieldUnique(new String[]{"yun-5-4a5cae51-fdd3-4e01-87c9-de6f4dc53254-applogfile-nginx-2016-12-22"}, "data.app_file");
//        ESSearch.queryString2(new String[]{"yun-5-4a5cae51-fdd3-4e01-87c9-de6f4dc53254-applogfile-nginx-2016-12-22"}, "*", new String[]{"data.app_file"},null);
//        ESSearch.queryString2(new String[]{"test-888"}, "*", new String[]{"data.app_file"},null);
//        SearchHit[] searchHits = ESSearch.queryString2(new String[]{"yun-5-4a5cae51-fdd3-4e01-87c9-de6f4dc53254-applogfile-nginx-2016-12-22"}, "*");

       /* for (SearchHit s : searchHits){
            System.out.println(s.getSource().toString());
        }*/
    }
//    @Test
    public void testSearchCustomLog() throws Exception{
        SearchHit[] queryString2 = ESSearch.queryString2(new String[]{"yun-customlog-*"},
                new String[]{"7039-c361c30b-527d-4bfd-b61f-cb4220345a32"},
                "*",
                new String[]{"data.app_file"}, null);
        for(SearchHit sh : queryString2){
            System.out.println(sh.getSource().toString());
        }
    }
}
