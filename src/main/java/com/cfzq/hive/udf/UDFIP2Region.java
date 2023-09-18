package com.cfzq.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Description(name = "ip2region", 
    value = "_FUNC_(str, segment[, key]) - return region via giving IP addresss", 
    extended = "segment: country, g, province, p, city, c, isp, i, "
      + " Example: \n"
      + " >SELECT ip2region('220.168.17.28'); \n" 
      + " 中国|0|北京|北京市|联通 \n"
      + " >SELECT ip2region('220.168.17.28', 'city'); \n"
      + " 北京市 \n"
    )

public class UDFIP2Region extends UDF {

  private  String url = "http://app.bigdata.cfzq.com:6001/v1/ipsearch/";

  public UDFIP2Region(String custUrl) {
    url = custUrl;
  }

  public UDFIP2Region() {}

  public String ipGet(String ip, String segment) throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = "empty content";
    try {
      String realUrl = segment == null ? url + ip : url + ip + "?q=" + segment;
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
              result = EntityUtils.toString(entity, "UTF-8");
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
    String r = ipGet(ip, null);
    return r;
  }

  public String evaluate(String ip, String segment) throws IOException{
    if (ip == null) {
        return null;
    }
    String r = ipGet(ip, segment);
    return r;
  }
}
