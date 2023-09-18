package com.cfzq.hive.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class DFSUtil {
 
    private org.apache.hadoop.conf.Configuration hadoopConf = null;
    private Boolean haveKerberos = true;
    private String kerberosKeytabFilePath = "/etc/security/keytabs/hive.service.keytab";
    private String kerberosPrincipal = "hive/_HOST@CFZQ.COM";


    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";


    public DFSUtil() {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        this.hadoopConf.set(HDFS_DEFAULTFS_KEY, "hdfs://cfzq");
        if (haveKerberos) {
            this.hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);

    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        if (haveKerberos) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                System.out.println(message);
                e.printStackTrace();
            }
        }
    }


    public InputStream getInputStream(String filepath) {
        InputStream inputStream;
        Path path = new Path(filepath);
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            //If the network disconnected, this method will retry 45 times
            //each time the retry interval for 20 seconds
            inputStream = fs.open(path);
            return inputStream;
        } catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filepath, filepath);
            System.out.println(message);
            e.printStackTrace();
            return null;
        }
    }


    public List<String> readLines(String filepath) throws IOException {
        Path pt = new Path(filepath);
        FileSystem fs = FileSystem.get(hadoopConf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        List<String> lines = new ArrayList<String>();
        try {
            String line;
            if (! fs.exists(pt)) {
                return new ArrayList<String>();
            }
            line=br.readLine();
            while (line != null){
              lines.add(line);
              // be sure to read the next line otherwise you'll get an infinite loop
              line = br.readLine();
            }
          } finally {
            // you should close out the BufferedReader
            br.close();
          }
        return lines;
    }
}