package com.cfzq.hive.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

/**
 * UDFIP2Region.
 */
public class TestUDFIP2Region {

  UDFIP2Region udf = new UDFIP2Region("http://localhost:8000/v1/ipsearch/");

  @Test
  public void TestUDFIP2RegionDomestic() throws IOException {
    String expectResult = "中国|0|山东省|青岛市|阿里云";
    String output = udf.evaluate("115.28.10.15");
    assertEquals("udf.evaluate('115.28.10.15')", expectResult, output!= null ? output: null);
  }

  @Test
  public void TestUDFIP2RegionBoard() throws IOException {
    String expectResult = new String("美国|0|德克萨斯|休斯顿|谷歌");
    String output = udf.evaluate("64.233.160.1");
    assertEquals("udf.evaluate('64.233.160.1')",expectResult, output!= null ? output: null);
  }
  
  @Test
  public void TestUDFIP2RegionSeg() throws IOException {
    String expectResult = "休斯顿";
    String output = udf.evaluate("64.233.160.1", "city");
    assertEquals("udf.evaluate('64.233.160.1', 'city')", expectResult, output!= null ? output: null);
  }
}
