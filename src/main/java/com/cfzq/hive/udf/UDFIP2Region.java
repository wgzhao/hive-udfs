package com.cfzq.hive.udf;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


@Description(name = "ip2region", 
    value = "_FUNC_(ip) - return region via giving IP addresss", 
    extended = "Example: \n"
      + " SELECT ip2region('220.168.17.28'); \n" 
      + "215|中国|0|北京|北京市|联通|18020")

public class UDFIP2Region extends UDF {

  private  String url = "http://app.bigdata.cfzq.com:6001/v1/ipsearch/";

  public UDFIP2Region(String custUrl) {
    url = custUrl;

  }
  public String ipGet(String ip, String segment) throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = "empty content";
    try {
      String realUrl = null;
      if (segment.equals("--")) {
        realUrl = url + ip;
      } else {
        realUrl = url + ip + "?q=" + segment;
      }
      HttpGet request = new HttpGet(realUrl);

      // add request headers
      //request.addHeader("content-type", "application/json");
      // request.addHeader(HttpHeaders.USER_AGENT, "Googlebot");
      CloseableHttpResponse response = httpClient.execute(request);
      try {
          // Get HttpResponse Status
          HttpEntity entity = response.getEntity();
          if (entity != null) {
              // return it as a String
              result = EntityUtils.toString(entity);
          }

      } finally {
          response.close();
      }
      } finally {
          httpClient.close();
      }
      return result.replace("\"", "");
  }


  public String evaluate(String ip) throws IOException{
    if (ip == null) {
        return null;
    }
    String r = ipGet(ip, "--");
    return r;
  }

  public String evaluate(String ip, String segment) throws IOException{
    if (ip == null) {
        return null;
    }
    String r = ipGet(ip, segment);
    return r;
  }
//   public Text evaluate(Text ip) {
//     if (ip == null) {
//         return new Text("no arguments");
//     }
//     String r = ipSearch(ip.toString());
//     result.set(r);
//     if (result == null) {
//         return new Text("not find");
//     } else {
//         return result;
//     }
//   }

  // public String evaluate(String ip, String segment) {
  //   if (ip == null || segment == null) {
  //       return null;
  //   }
  //   String[] output = ipSearch(ip).split("\\|");
  //   //"中国|0|山东省|青岛市|阿里云"
  //   //"美国|0|德克萨斯|休斯顿|谷歌
  //   Integer seg = segMap.get(segment); 
  //   //result.set(output[seg]);
  //   return output[seg];
  // }

//   public Text evaluate(Text ip, Text segment) {
//     if (ip == null || segment == null) {
//         return null;
//     }
//     String[] output = ipSearch(ip.toString()).split("\\|");
//     //"中国|0|山东省|青岛市|阿里云"
//     //"美国|0|德克萨斯|休斯顿|谷歌
//     Integer seg = segMap.get(segment.toString()); 
//     result.set(output[seg]);
//     return result;
//   }
}
