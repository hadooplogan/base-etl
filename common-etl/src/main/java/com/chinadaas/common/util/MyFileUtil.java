package com.chinadaas.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class MyFileUtil {

    public static boolean copyMergeWithHeader(Path srcDir, Path dstFile, String header,boolean deleteSource) throws IOException {

        System.out.println("srcDir getname"+dstFile.getName());
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

   /* public static DataFrame dropHeader(DataFrame df){

        df.toJavaRDD().mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<Row>>(){
            @Override
            public Iterator<Row> call(Integer ind, Iterator<Row> iterator) throws Exception {
                if(ind==0 && iterator.hasNext()){
                    iterator.next();
                    return iterator;
                }else
                    return iterator;
            }
        },false).;

    }*/

}
