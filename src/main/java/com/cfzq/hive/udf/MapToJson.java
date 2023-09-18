package com.account.analyzer.hadoop.udf.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhengtx19755 on 2019/8/6.
 */
public class MapToJson extends UDF {
    public static String evaluate(final Map<String,String> map) {

        return JSONObject.toJSONString(map);
    }

    public static void main(String[] args) {
        Map<String,String> map = new HashMap<>();
        map.put("name","zhengtx");
        map.put("age","20");
        System.out.println(map.toString());
        System.out.println(JSONObject.toJSONString(map));
    }

}
