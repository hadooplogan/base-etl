package com.chinadaas.recommend.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author haoxing
 * 从新构建训练集，根据顾客提供的正例数据，自动补充相同数量的负例。可以为初次上传名单的客户使用这个训练数据集。
 */
public class UnionBuildTrainningSet {

    static EsDataAdapterImpl ida = new EsDataAdapterImpl();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                appName("build_true_traningset").
                enableHiveSupport().
                getOrCreate();

        HashMap<String, String> cfg = new HashMap<>();
        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id", "pripid");
        cfg.put("es.resource.read", "ent_index/ENT_INDEX");

        /**
         * 从es拿出全量数据
          */
        JavaRDD allpripid = getAllpripid(spark, cfg);
        /**
         * 拿出负例样本并拿出(和全量数据求得差集)
         */
        JavaPairRDD ngd = getNGD(spark, allpripid);

        /**
         * 从hdfs上拿到正例数据集，拿到label
         */
        JavaPairRDD<String, String> pnc = getPNC(spark);

        /**
         * 组成新的训练数据
         */
        JavaPairRDD<String, String> trainSet = getTrainSet(spark, pnc, ngd);

        /**
         * 从es中拿到维度特征
         */
        JavaPairRDD feature = getFeature(spark, cfg);


        /**
         * 得到训练集
         */
        JavaPairRDD trainData = getTrainData(trainSet, feature);

        /**
         *保存训练集到hdfs
         */
        String targetDir = "/tmp/spark_test/test_data";
        trainingDataArrange(spark,trainData,targetDir,false);

        spark.stop();
    }

    /**
     * 从es里拿出全量数据
     */

    public static JavaRDD getAllpripid(SparkSession spark, Map cfg) {
        Dataset dataset = (Dataset) ida.loadData(spark, cfg);
        Dataset select = dataset.select("pripid");
        JavaRDD javaRDD = select.toJavaRDD(); 
        return javaRDD;
    }

    /**
     * 读出客户名单的pripid和全量集做差集，拿出不相同的一定量数据,打上label。
     */
    public static JavaPairRDD getNGD(SparkSession spark, JavaRDD data) {
        JavaRDD<String> customerRdd = spark.sparkContext().textFile("/tmp/spark_test/train_data_list.csv", 10).toJavaRDD().
                map(new Function<String, String>() {
                    @Override
                    public String call(String v1) throws Exception {
                        String[] split = v1.split("\t");
                        String s = split[0];
                        return s;
                    }
                });
        long count = customerRdd.count();
        int count1 = (int) count;
        JavaRDD subtract = data.subtract(customerRdd);
        List take = subtract.take(count1);
        //java解决从2.0版本后.因是因为由上面代码实例化的spark调用sparkContext()方法获取的context对象是scala的SparkContext对象，而不是我们最开始的手动方法获取的JavaSparkContext对象。
        JavaRDD javaRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(take);

        JavaPairRDD javaPairRDD = javaRDD.map(new Function() {
            @Override
            public Object call(Object v1) throws Exception {

                String s = v1.toString();

                String ng = s + "\t" + "1";

                return ng;
            }
        }).mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                String v = o.toString();
                String[] split = v.split("\t");
                return new Tuple2(split[0], split[1]);
            }
        });
        return javaPairRDD;
    }

    /**
     * 去es库中拿到维度特征。
     */
    public static JavaPairRDD getFeature(SparkSession spark, Map cfg) {
        Dataset dataset = (Dataset) ida.loadData(spark, cfg);
        Dataset select = dataset.select("pripid", "ee0001", "eb0001", "eb0002", "ee0011", "eb0003", "eb0004", "eb0005", "ee0007");
        JavaPairRDD<String, String> pairRDD = select.toJavaRDD().mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                String[] split = o.toString().replace("[", "").replace("]", "").split(",");
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 1; i < split.length; i++) {
                    stringBuilder.append(split[i]).append("|");
                }
                return new Tuple2(split[0], stringBuilder.toString().substring(0, stringBuilder.toString().length() - 1));
            }
        });
        return pairRDD;
    }

        /**
         * 拿到正例数据和label
         */
        public static JavaPairRDD<String, String> getPNC (SparkSession spark){

            JavaRDD<String> rdd = spark.sparkContext().textFile("/tmp/spark_test/train_data_list.csv", 10).toJavaRDD();
            JavaPairRDD<String, String> javaPairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] split = s.split("\t");
                    return new Tuple2<>(split[1], split[12]);
                }
            });
            return javaPairRDD;
        }
    


        /**
         * 把正例数据和反例数据组成新的数据集
         */
     
        public static  JavaPairRDD<String,String> getTrainSet(SparkSession spark,JavaPairRDD pnc,JavaPairRDD ngd){

            JavaPairRDD union = pnc.union(ngd);
           return union;
        }

    /**
     *
     * 得到训练集
     * @param pncRDD
     * @param featureRDD
     * @return
     */
    public static JavaPairRDD<String, Tuple2<String, String>> getTrainData(JavaPairRDD<String,String> pncRDD, JavaPairRDD<String,String> featureRDD) {
        JavaPairRDD<String, Tuple2<String, String>> pairRDD = pncRDD
                .leftOuterJoin(featureRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<String>>>, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, Optional<String>>> stringTuple2) throws Exception {
                        String PNC = stringTuple2._2._1;
                        String feature = stringTuple2._2._2.orElse("");
                        return new Tuple2<>(stringTuple2._1, new Tuple2<>(PNC, feature));
                    }
                }).filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, String>> v1) throws Exception {
                        String[] split = v1._2._2.split("\\|");
                        Boolean boo = false;
                        if (split.length >= 7) {
                            boo = true;
                        }
                        for (String str : split) {
                            if (str == null || str.equals("") || str.equals("null") || str.equals("NULL") || str.equals("Null") || str.equals("!")) {
                                boo = false;
                            }
                        }
                        return boo;
                    }
                });
        return pairRDD;
    }


    /**
     * 得到训练集并存入hdfs
     * @param spark
     * @param javaRDD
     * @param targetDir
     * @param recommend
     */
    public static void trainingDataArrange(SparkSession spark, JavaPairRDD<String, Tuple2<String, String>> javaRDD, String targetDir, final Boolean recommend) {

        final Broadcast<Boolean> broadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(recommend);

        JavaRDD<String> mapRDD = javaRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {

                return new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2);
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                Boolean recommend = broadcast.value();
                String label = v1._1();
                if (recommend == true) {
                    label = "0";
                }
                String[] feature = v1._2().split("\\|");
                String EE001 = feature[0].replace("0000", "");
                String EB001 = feature[1];
                if (EB001.equals("99")) {
                    EB001 = "5";
                } else {
                    EB001 = EB001.replace("0", "");
                }
                String EB002 = feature[2];
                if (EB002.equals("99")) {
                    EB002 = "4";
                } else {
                    EB002 = EB002.replace("0", "");
                }
                int EE011 = feature[3].toLowerCase().charAt(0) - 96;
                String EB003 = feature[4];
                if (EB003.equals("99")) {
                    EB003 = "5";
                } else {
                    EB003 = EB003.replace("0", "");
                }
                String EB004 = feature[5];
                String EB005 = feature[6];
                if (EB005.equals("99")) {
                    EB005 = "5";
                } else {
                    EB005 = EB005.replace("0", "");
                }
                String EE007 = feature[7];
                StringBuilder builder = new StringBuilder();
                builder.append(label).append(" ")
                        .append("1:").append(EE001).append(" ")
                        .append("2:").append(EB001).append(" ")
                        .append("3:").append(EB002).append(" ")
                        .append("4:").append(EE011).append(" ")
                        .append("5:").append(EB003).append(" ")
                        .append("6:").append(EB005).append(" ")
                        .append("7:").append(EE007).append(" ")
                        .append("8:").append(EB004);

                return builder.toString();
            }
        });
        Path path = null;
        Configuration entries = null;
        FileSystem fileSystem = null;
        try {
            path = new Path(targetDir);
            entries = spark.sparkContext().hadoopConfiguration();
            fileSystem = FileSystem.get(entries);
            if(fileSystem.exists(path)){
                //递归删除目录
                fileSystem.delete(path,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        mapRDD.saveAsTextFile(targetDir);
    }
    }
