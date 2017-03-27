package com.hna.es.util;

import java.lang.annotation.ElementType;
import java.text.DecimalFormat;

/**
 * Created by GH-GAN on 2017/2/27.
 */
public class UnitChangeUtil {
    public static DecimalFormat df = new DecimalFormat("#.00");
    public static double toMb(String sizeStr){
        String _sizeStr = sizeStr.toLowerCase();
        if(_sizeStr.endsWith("t")){
            double _t = Double.parseDouble(sizeStr.trim().substring(0, sizeStr.length() - 1));
            return _t * 1024 * 1024;
        }
        if(_sizeStr.endsWith("gb")){
            double _gb = Double.parseDouble(sizeStr.trim().substring(0, sizeStr.length() - 2));
            return _gb * 1024;
        }
        if(_sizeStr.endsWith("mb")){
            double _mb = Double.parseDouble(sizeStr.trim().substring(0, sizeStr.length() - 2));
            return _mb;
        }
        if(_sizeStr.endsWith("kb")){
            double _kb = Double.parseDouble(sizeStr.trim().substring(0, sizeStr.length() - 2));
            return _kb / 1024.0;
        }
        return -1;
    }
    public static String mbToFitUnit(double size){
        if (size >= 1048576) return df.format(size / 1024 / 1024) + "T";
        else if (size >= 1024) return df.format(size / 1024) + "GB";
        else if (size < 1) return df.format(size * 1024) + "KB";
        return df.format(size) + "MB";
    }
}
