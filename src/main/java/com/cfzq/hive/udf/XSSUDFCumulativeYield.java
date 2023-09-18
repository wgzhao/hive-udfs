/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cfzq.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLength;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * UDFLength.
 *
 */
@Description(name = "udf_cumulative_yield",
    value = "_FUNC_(List<assetText>,List<lastAssetText>,List<fundText>,List<dateText>,String fundAccount) - Returns the length of str or number of bytes in binary data",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(List<assetText>,List<lastAssetText>,List<fundText>,List<dateText>,String fundAccount) FROM src LIMIT 1;\n" + "  8")
@VectorizedExpressions({StringLength.class})
public class XSSUDFCumulativeYield extends UDF {

    public HiveVarcharWritable evaluate(Text assetText,Text lastAssetText,Text fundText,Text dateText,Text fundAccount) throws HiveException {
        HiveVarcharWritable result = new HiveVarcharWritable();

        if (assetText == null || assetText.toString().trim().isEmpty()) return null;
        if (lastAssetText == null || lastAssetText.toString().trim().isEmpty()) return null;
        if (fundText == null || fundText.toString().trim().isEmpty()) return null;
        if (dateText == null || dateText.toString().trim().isEmpty()) return null;
        String[] assetArray = assetText.toString().split(",");
        List<BigDecimal> assetDa = new ArrayList<>();

        String[] lastAssetArray = lastAssetText.toString().split(",");
        List<BigDecimal> lastAssetDa = new ArrayList<>();

        String[] fundArray = fundText.toString().split(",");
        List<BigDecimal> fundDa = new ArrayList<>();

        String[] dateArray = dateText.toString().split(",");
        List<String> dateDa = new ArrayList<>();

        try {
            for (String s : assetArray) {
                if (s == null || s.trim().isEmpty()) {
                    assetDa.add(BigDecimal.ZERO);
                }
                else {
                    assetDa.add(new BigDecimal(s));
                }
            }
            for (String s : lastAssetArray) {
                if (s == null || s.trim().isEmpty()) {
                    lastAssetDa.add(BigDecimal.ZERO);
                }
                else {
                    lastAssetDa.add(new BigDecimal(s));
                }
            }
            for (String s : fundArray) {
                if (s == null || s.trim().isEmpty()) {
                    fundDa.add(BigDecimal.ZERO);
                }
                else {
                    fundDa.add(new BigDecimal(s));
                }
            }
            Collections.addAll(dateDa, dateArray);
        } catch (Exception e) {
            throw new HiveException("参数中包含非数字", e);
        }

        try {
            List<BigDecimal> yieldList =  calCumulativeYield(assetDa,lastAssetDa,fundDa);
            String str = joinColumn(yieldList,dateDa,fundAccount.toString());
//            System.out.println(str);
            result.set(str);
            return result;
        }catch (Exception e){
            throw new HiveException(fundAccount + "=====资金账号计算收益率报错", e);
        }
    }

    /**
     *
     * @param yieldList
     * @param dateList
     * @param fundAccount
     * @return
     */
  public String joinColumn(List<BigDecimal> yieldList,List<String> dateList,String fundAccount){
      List<String> lineList = new ArrayList<>();
      for(int i = 0; i < dateList.size(); i++){
          String line = fundAccount + "!#" + dateList.get(i) + "!#" + yieldList.get(i);
          lineList.add(line);
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < lineList.size(); i++) {

          sb.append(lineList.get(i));
          if(lineList.size()>i+1){
              sb.append("!@#");
          }
      }
      return sb.toString();
  }

  public List<BigDecimal> calCumulativeYield(List<BigDecimal> assetList,List<BigDecimal> lastAssetList,List<BigDecimal> fundList){
      List<BigDecimal> yieldList = new ArrayList<>();
      //份数集合
      List<BigDecimal> share_list = new ArrayList<>();
      //净值集合
      List<BigDecimal> net_value_list = new ArrayList<>();

      //上日净值
      BigDecimal last_net_value = BigDecimal.ONE;
      //上日份数
      BigDecimal last_share = BigDecimal.ZERO;
      BigDecimal begin_net_value = BigDecimal.ZERO;
      for (int i = 0; i < assetList.size(); i++) {
          //今日净流入
          BigDecimal net_fund = BigDecimal.ZERO;
          if(fundList.size()-1>=i){
              net_fund = fundList.get(i);
          }
          if(i==0){
              last_share = lastAssetList.get(i);
          }else{
              last_share = share_list.get(i-1);
          }
          BigDecimal today_share = BigDecimal.ZERO;
          //当日份数 = 上日份数 + （当日转账净流入 / 上日净值）
          if(null==last_net_value || BigDecimal.ZERO.compareTo(last_net_value)==0){
              today_share = last_share.add(net_fund);
          }else{
              today_share = last_share.add(net_fund.divide(last_net_value,6,BigDecimal.ROUND_HALF_UP));
          }
          //当日净值 = 当日资产 / 当日份数;
          BigDecimal today_net_value = BigDecimal.ZERO;
          if(BigDecimal.ZERO.compareTo(today_share) != 0){
              today_net_value = assetList.get(i).divide(today_share,6,BigDecimal.ROUND_HALF_UP);
          }else{
              //今日份数为0，则今日净值=上日净值
              today_net_value = last_net_value;
          }
          //今日资产为0，则今日净值=上日净值
          if(null==assetList.get(i) || BigDecimal.ZERO.compareTo(assetList.get(i))==0){
              today_net_value = last_net_value;
          }

          share_list.add(today_share);
          net_value_list.add(today_net_value);

          if(i>=1 && begin_net_value.compareTo(BigDecimal.ZERO)==0){
              begin_net_value = today_net_value;
          }
//          if(begin_net_value.compareTo(BigDecimal.ZERO)==0){
//          }
          BigDecimal end_net_value = net_value_list.get(net_value_list.size()-1);
          BigDecimal yield = BigDecimal.ZERO;
          if(begin_net_value.compareTo(BigDecimal.ZERO)!=0 && end_net_value.compareTo(BigDecimal.ZERO)!=0){
              yield = end_net_value.divide(begin_net_value,6,BigDecimal.ROUND_HALF_UP).subtract(BigDecimal.ONE);
          }
          System.out.println(yield);
          yieldList.add(yield);
          //本次循环结束，今日净值变成上日净值
          last_net_value = today_net_value;
      }
      return yieldList;
  }


    public static void main(String[] args) {
        try {
            new XSSUDFCumulativeYield().evaluate(new Text("10000,11000,12000,13000,14000,15000,10000"),
                    new Text("5000,10000,11000,12000,13000,14000,15000"),new Text("5000,0,0,0,0,0,-5000"),
                            new Text("20201101,20201102,20201103,20201104,20201105,20201107,20201108"),new Text("30001200"));

        } catch (HiveException e) {
            e.printStackTrace();
        };
    }
}
