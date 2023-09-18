package com.cfzq.hive.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.cfzq.hive.util.OSUtils;

/**
 * Created by tianrui on 17/5/3.
 */

/**
 * func：hadoop集群连接配置
 * hadoop/core-sit.xm：配置hadoop集群的主名称节点
 */
public class HadoopConf {

    private static Configuration conf = new Configuration();
    private static Configuration hadoopConfig = null;

    static {
            Map<String, String> map = System.getenv();
            String hadoop_home = map.get("HADOOP_CONF_DIR");
            if(StringUtils.isBlank(hadoop_home)){
                hadoop_home = "/etc/hadoop/conf/core-site.xml";
            }
            hadoopConfig = new Configuration();
            hadoopConfig.set("hadoop.core.site.path", hadoop_home);
            Path hadoopCoreSitePath = new Path(hadoopConfig.get("hadoop.core.site.path", hadoop_home));
            if (OSUtils.osType == OSUtils.OSType.OS_TYPE_WIN
                    || OSUtils.osType == OSUtils.OSType.OS_TYPE_MAC) {//开发环境
                conf.addResource("hadoop/core-site.xml");
            } else {
                conf.addResource(hadoopCoreSitePath);//非开发环境
            }
            conf.set("fs.hdfs.impl",
                    hadoopConfig.get("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"));
    }

    private HadoopConf() {
        //限制构造新实例
    }

    public static Configuration getInstance() {
        return conf;
    }

    public static void main(String[] args) {
        Map<String, String> map = System.getenv();
//        for (Iterator<String> itr = map.keySet().iterator(); itr.hasNext();) {
//            String key = itr.next();
//            System.out.println(key + "=" + map.get(key));
//        }
        System.out.println(map.get("HADOOP_HOME"));;
    }
}
