package com.chinadaas.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MyFileUtil {

    public static boolean copyMergeWithHeader(String srcDirstr, String dstFilestr, String fileName,String header,boolean deleteSource) throws IOException {

        Path srcDir = new Path(srcDirstr);
        Path dstFile = new Path(dstFilestr+"/"+fileName);
        System.out.println("srcDir getname=="+dstFilestr+"/"+fileName);
        FileSystem hdfs = null;
        Configuration hadoopConfig = new Configuration();
        DataFormatConvertUtil.deletePath(dstFile.getName());
        try{
            hdfs = FileSystem.get(hadoopConfig);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(hdfs != null){
                try {
                    hdfs.closeAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if(!hdfs.getFileStatus(srcDir).isDir()) {
            return false;
        } else {
            FSDataOutputStream out = hdfs.create(dstFile);
            if(header != null) {
                out.write((header + "\n").getBytes("UTF-8"));
            }

            try {
                FileStatus[] contents = hdfs.listStatus(srcDir);

                for(int i = 0; i < contents.length; ++i) {
                    if(!contents[i].isDir()) {
                        FSDataInputStream in = hdfs.open(contents[i].getPath());

                        try {
                            IOUtils.copyBytes(in, out, hadoopConfig, false);

                        } finally {
                            in.close();
                        }
                    }
                }
            } finally {
                out.close();
            }

            return deleteSource?hdfs.delete(srcDir, true):true;
        }
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException {
        if(dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if(sdst.isDir()) {
                if(null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }

                return checkDest((String)null, dstFS, new Path(dst, srcName), overwrite);
            }

            if(!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }

        return dst;
    }


    /**
     * 读取txt文件的内容
     * @param file 想要读取的文件对象
     * @return 返回文件内容
     */
    public static Map getFileCfg(String file){
        Map<String,String> cfg = new HashMap<String,String>();

        try{
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                s.substring(0,s.indexOf(","));
                cfg.put(s.substring(0,s.indexOf(",")),s.substring( s.lastIndexOf(",")+1).replace("\\",""));
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return cfg;
    }

    //拿到分区表日期



    public static Map getDateCfg(String file){
        Map<String,String> cfg = new HashMap<String,String>();


        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null) {//使用readLine方法，一次读一行
                s.substring(0, s.indexOf(","));
                String s1 = s.substring(s.lastIndexOf(",") + 1);
                if (s1.contains("=")) {
                    String[] split = s1.split("=");
                    cfg.put(s.substring(0, s.indexOf(",")), split[1]);
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cfg;
    }



/*    public static Dataset dropHeader(Dataset df){






        df.toJavaRDD().mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<Row>>(){
            @Override
            public Iterator<Row> call(Integer ind, Iterator<Row> iterator) throws Exception {
                if(ind==0 && iterator.hasNext()){
                    iterator.next();
                    return iterator;
                }else
                    return iterator;
            }
        },false);

    }*/

}
