package com.hna.es.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by GH-GAN on 2016/11/24.
 */
public class DateUtil {

    public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static SimpleDateFormat df_utc_1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    public static SimpleDateFormat df_utc_2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public static SimpleDateFormat df_utc_3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public static SimpleDateFormat df_local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    public static SimpleDateFormat df_utc_base3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.FFFFFFSSS'Z'");

    public static  TimeZone utc_0 = TimeZone.getTimeZone("UTC");

    static {
        df_utc_1.setTimeZone(utc_0);
        df_utc_2.setTimeZone(utc_0);
        df_utc_3.setTimeZone(utc_0);
        df_utc_base3.setTimeZone(utc_0);
    }

    public static String formatToUTC_0(String timestap){
        try {
            Date parse = df.parse(timestap);
           return df_utc_2.format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String formatToGeneral(String timestap){
        try {
            Date parse = df_utc_2.parse(timestap);
            return df.format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String baseUTCToGeneral(String timestap){
        try {
            Date parse = df_utc_1.parse(timestap);
            return df.format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getyyyyMMdd(String timestamp){
        return timestamp.substring(0,"yyyy-MM-dd".length());
    }

    public static Date parseLocalDate(String timestamp) throws ParseException {
        return df_local.parse(timestamp);
    }
}
