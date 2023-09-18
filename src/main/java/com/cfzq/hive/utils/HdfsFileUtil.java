package com.cfzq.hive.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 导出到文件
 * 
 * @author qinfan
 *
 */
public class HdfsFileUtil {
    
    Logger log = Logger.getLogger(HdfsFileUtil.class);
    Configuration conf = HadoopConf.getInstance();
    
    public static void main(String[] args) throws IOException {
//        HdfsFileUtil fh = new HdfsFileUtil();
//        List<String> list = fh.readLinesDictionary();
//        for (String item : list) {
//            System.out.println(item);
//        }
        
    }
    
    /**
     * 读取hdfs文件
     * 
     * @Title: readLines
     * @Description: TODO
     * @param path
     * @return
     * @return: List<String>
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public List<String> readLines(String path) throws IllegalArgumentException, IOException {
        
        List<String> resule = new ArrayList<String>();

        FileSystem fs = FileSystem.get(URI.create(path), conf);
        InputStream in = null;
        try {
            Path paths = new Path(path);
            if (!fs.exists(paths)) {
                return new ArrayList<String>();
            }
            if (fs.isDirectory(paths)) {
                FileStatus[] status = fs.listStatus(paths);
                for (FileStatus file : status) {
                    if (file.getPath().getName().endsWith(".crc")) {
                        continue;
                    }
                    in = fs.open(new Path(file.getPath().toString()));
                    resule.addAll(IOUtils.readLines(in));
                }
                return resule;
            } else {
                in = fs.open(new Path(path));
                resule.addAll(IOUtils.readLines(in));
                return resule;
            }
        } finally {
            IOUtils.closeQuietly(in);
        }
    }


}
