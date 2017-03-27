package com.hna.es.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by GH-GAN on 2017/2/27.
 */
public class ESConfig {
    public static String nodes = null;
    public static String transportPort = null;
    public static String httpPort = null;
    public static String clusterName = null;
    static {
         InputStream inputStream = ESClient.class.getResourceAsStream("/conf.properties");
//        InputStream inputStream = ESClient.class.getResourceAsStream("/conf_docker.properties");
//        InputStream inputStream = ESClient.class.getResourceAsStream("/conf_aws.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
            nodes = properties.getProperty("nodes");
            transportPort = properties.getProperty("transportPort");
            httpPort = properties.getProperty("httpPort");
            clusterName = properties.getProperty("clusterName");
        } catch (Exception e) {
            System.err.println("conf file load error !!!");
            e.printStackTrace();
        }
    }
}
