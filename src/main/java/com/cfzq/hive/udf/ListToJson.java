package com.cfzq.hive.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengtx19755 on 2019/8/6.
 */
public class ListToJson extends UDF {
    public static String evaluate(final List<String> list) {

//        JSONArray jsonArray = new JSONArray();
//        jsonArray.add(list);

//        return JSONArray.toJSONA(list);
        return JSONArray.parseArray(list.toString()).toString();
    }

    public static void main(String[] args) {
        Map<String,String> map = new HashMap<>();
        map.put("name","zhengtx");
        map.put("age","20");
        System.out.println(map.toString());
        List<String> list = new ArrayList<>();
        list.add(JSONObject.toJSONString(map));
        System.out.println(list);
        System.out.println(new ListToJson().evaluate(list));
    }

}
