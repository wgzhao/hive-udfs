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

@Description(name = "mobile2region", 
    value = "_FUNC_(str, segment[, key]) - return region according to mobile phone", 
    extended = "segment: province, p, city, c, isp, i, "
      + " Example: \n"
      + " >SELECT mobile2region('13573121234'); \n" 
      + " 山东|济南|移动 \n"
      + " >SELECT mobile2region('13573121234', 'city'); \n"
      + " 济南 \n"
    )

public class UDFMobile2Region extends UDF {

  private  String url = "http://app.bigdata.cfzq.com:6001/v1/msearch/";

  public UDFMobile2Region(String custUrl) {
    url = custUrl;
  }

  public UDFMobile2Region() {}

  public String mobileSearch(String phone, String segment) throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = "empty content";
    try {
      String realUrl = segment == null ? url + phone : url + phone + "?q=" + segment;
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

  public String evaluate(String phone) throws IOException{
    if (phone == null) {
        return null;
    }
    String r = mobileSearch(phone, null);
    return r;
  }

  public String evaluate(String phone, String segment) throws IOException{
    if (phone == null) {
        return null;
    }
    String r = mobileSearch(phone, segment);
    return r;
  }
}
