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