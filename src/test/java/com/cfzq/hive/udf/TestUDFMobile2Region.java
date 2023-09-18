package com.cfzq.hive.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

/**
 * UDFMobile2Region.
 */
public class TestUDFMobile2Region {

  UDFMobile2Region udf = new UDFMobile2Region();

  @Test
  public void TestUDFMobile2RegionDomestic() throws IOException {
    String expectResult = "山东|济南|移动";
    String output = udf.evaluate("13573121234");
    assertEquals("udf.evaluate('13573121234')", expectResult, output!= null ? output: null);
  }
  
  @Test
  public void TestUDFMobile2RegionSeg() throws IOException {
    String expectResult = "济南";
    String output = udf.evaluate("13573121234", "city");
    assertEquals("udf.evaluate('13573121234', 'city')", expectResult, output!= null ? output: null);
  }
}
