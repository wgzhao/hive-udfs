# 自定义的Hive UDF仓库

## 当前函数

- `ip2region` 指定IP地址及需要返回的层级（可选)，返回IP所在归属地
- `mobile2region` 指定移动好吗以及返回的层级(可选)，返回手机号码所在归属地

## 安装

```sql
create function ip2region as 'com.cfzq.hive.udf.UDFIP2Region' using jar 'hdfs://cfzq/udf/cfzq-hiveudfs.jar';
create function mobile2region as 'com.cfzq.hive.udf.UDFMobile2Region' using jar 'hdfs://cfzq/udf/cfzq-hiveudfs.jar';
```

## 使用

详细使用，请参考[这里](https://gitlab.ds.cfzq.com/grp_ds/edw/-/blob/master/docs/54-edw-com_ip_region.md)

## 恒生账户分析UDF函数

```sql
create schema if not exists cs_udf;
use cs_udf;
drop function cs_udf.add_to_date_day;
drop function cs_udf.FillDataByClosedayUDF;
drop function cs_udf.FillDataByClosedayUDF;  
drop function cs_udf.GetBetExchangeDaysNum;
drop function cs_udf.ListToJson;
drop function cs_udf.MapToJson;
drop function cs_udf.udaf_close_avg_add_first;
drop function cs_udf.udaf_close_avg;
drop function cs_udf.udaf_close_sum;
drop function cs_udf.udaf_cross_sum_by_date;
drop function cs_udf.udaf_prev_close_value;
drop function cs_udf.udaf_prev_value;
drop function cs_udf.udaf_recent_close_avg_add_first;
drop function cs_udf.udaf_recent_close_avg_add_prev_first;
drop function cs_udf.udaf_recent_close_avg;
drop function cs_udf.udaf_recent_close_sum;
drop function cs_udf.udaf_sub_by_date;
drop function cs_udf.udf_add_close_day;
drop function cs_udf.udf_add_hs_date;
drop function cs_udf.udf_add_normal_day;
drop function cs_udf.udf_collect_order_list;  
drop function cs_udf.udf_count_close_day;
drop function cs_udf.udf_from_hs_date;
drop function cs_udf.udf_hs_date_diff;
drop function cs_udf.udf_is_close_day;
drop function cs_udf.udf_last_close_day;
drop function cs_udf.udf_max_draw_down;
drop function cs_udf.udf_max_split_str;
drop function cs_udf.udf_month_first_close_day;
drop function cs_udf.udf_prev_year_close_day;
drop function cs_udf.udf_to_hs_date;
drop function cs_udf.udf_week_first_close_day;
drop function cs_udf.udf_year_first_close_day;
create function cs_udf.add_to_date_day as 'com.cfzq.hive.udf.XSSUDFIsCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.FillDataByClosedayUDF as 'com.cfzq.hive.udf.FillDataByClosedayUDF' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.GetBetExchangeDaysNum as 'com.cfzq.hive.udf.GetBetExchangeDaysNum' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.ListToJson as 'com.cfzq.hive.udf.ListToJson' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.MapToJson as 'com.cfzq.hive.udf.MapToJson' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_close_avg as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCloseAvg' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_close_avg_add_first as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCloseAvgAddFirst' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_close_sum as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCloseSum' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_collect_order_list as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCollectList' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_cross_sum_by_date as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCrossSumByDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_prev_close_value as 'com.cfzq.hive.udf.generic.XSSGenericUDAFPrevCloseValue' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_prev_value as 'com.cfzq.hive.udf.generic.XSSGenericUDAFPrevValue' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_recent_close_avg as 'com.cfzq.hive.udf.generic.XSSGenericUDAFRecentCloseAvg' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_recent_close_avg_add_first as 'com.cfzq.hive.udf.generic.XSSGenericUDAFRecentCloseAvgAddFirst' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_recent_close_avg_add_prev_first as 'com.cfzq.hive.udf.generic.XSSGenericUDAFRecentCloseAvgAddPrevFirst' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_recent_close_sum as 'com.cfzq.hive.udf.generic.XSSGenericUDAFRecentCloseSum' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udaf_sub_by_date as 'com.cfzq.hive.udf.generic.XSSGenericUDAFSubByDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_add_close_day as 'com.cfzq.hive.udf.XSSUDFAddCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_add_hs_date as 'com.cfzq.hive.udf.XSSUDFAddHsDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_add_normal_day as 'com.cfzq.hive.udf.XSSUDFAddNormalDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_collect_order_list as 'com.cfzq.hive.udf.generic.XSSGenericUDAFCollectList' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_count_close_day as 'com.cfzq.hive.udf.XSSUDFCountCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_from_hs_date as 'com.cfzq.hive.udf.XSSUDFFromHsDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_hs_date_diff as 'com.cfzq.hive.udf.XSSUDFHsDateDiff' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_is_close_day as 'com.cfzq.hive.udf.XSSUDFIsCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_last_close_day as 'com.cfzq.hive.udf.XSSUDFLastCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_max_draw_down as 'com.cfzq.hive.udf.XSSUDFMaxDrawDown' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_max_split_str as 'com.cfzq.hive.udf.XSSUDFMaxSplitStr' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_month_first_close_day as 'com.cfzq.hive.udf.XSSUDFWeekFirstCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_prev_year_close_day as 'com.cfzq.hive.udf.XSSUDFPrevYearCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_to_hs_date as 'com.cfzq.hive.udf.XSSUDFToHsDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_week_first_close_day as 'com.cfzq.hive.udf.XSSUDFAddHsDate' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
create function cs_udf.udf_year_first_close_day as 'com.cfzq.hive.udf.XSSUDFYearFirstCloseDay' using jar 'hdfs:///udf/cfzq-hiveudfs.jar';
```