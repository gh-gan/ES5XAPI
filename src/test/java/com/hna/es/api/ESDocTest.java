package com.hna.es.api;

import com.hna.es.bean.IndexTypeProperties;
import org.junit.Test;

import java.util.List;

/**
 * Created by GH-GAN on 2017/2/7.
 */
public class ESDocTest {
//    @Test
    public void testDeleteTyep() {
        long l = ESDoc.deleteType(new String[]{"test77-7"}, "type3");
        System.out.println(l);
    }
}
