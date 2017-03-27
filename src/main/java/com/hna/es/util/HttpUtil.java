package com.hna.es.util;

import org.apache.log4j.Logger;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by GH-GAN on 2016/11/28.
 */
public class HttpUtil {
    URLConnection conn = null;
    private static Logger logger = Logger.getLogger(HttpUtil.class);

    public HttpURLConnection init(String url){
        HttpURLConnection con = null;
        try {
            conn = new URL(url).openConnection();
            con = (HttpURLConnection) conn;
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            con.setRequestProperty("Accept-Charset", "utf-8");
            con.setDoOutput(true);
//            con.setDoInput(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    public void Post(String url,String json){
        HttpURLConnection con =  init(url);
        try {
            //写数据
            OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());;
            out.write(json);
            out.flush();

            logger.info("alert: \n" + json);
            logger.info("code: \n" + con.getResponseCode());

        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            con.disconnect();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static List<String> Get(String url){
        HttpURLConnection con2 = null;
        List<String> sizes = new LinkedList<String>();
        try {
            con2 = (HttpURLConnection) new URL(url).openConnection();
            con2.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(con2.getInputStream()));
            String line = null;

            while ((line = in.readLine()) != null){
                sizes.add(line.trim());
            }

        }catch (Exception e) {
//            e.printStackTrace();
            con2.disconnect();
        }
        return sizes;
    }
}
