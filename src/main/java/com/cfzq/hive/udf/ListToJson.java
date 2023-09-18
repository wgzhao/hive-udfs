package com.cfzq.hive.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengtx19755 on 2019/8/6.
 */
@Description(name = "ListToJson", 
    value = "_FUNC_(str) - convert map to json format", 
    extended = "segment: country, g, province, p, city, c, isp, i, "
        + "select 11 as fund_account,init_date,income,map( \n"
        + "\"init_date\",init_date, \n"
        + "\"income\",income \n"
        + ") as bb from ( \n"
        + "select 20200601 as init_date,\"11\" as income \n"
        + "union all \n"
        + "select 20200602 as init_date,\"22\" as income \n"
        + "union all \n"
        + "select 20200603 as init_date,\"33\" as income \n"
        + ") a  \n"
        + ") a; \n"
        + "{'income':'11','init_date':'20200601'}	11\n"
        + "{'income':'22','init_date':'20200602'}	11\n"
        + "{'income':'33','init_date':'20200603'}	11"
    )

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
