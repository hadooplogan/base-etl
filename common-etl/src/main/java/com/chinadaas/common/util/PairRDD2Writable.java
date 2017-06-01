package com.chinadaas.common.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface PairRDD2Writable {

	JavaPairRDD<Text, Writable> pairRDD2Hdfs(JavaSparkContext jsc);

}