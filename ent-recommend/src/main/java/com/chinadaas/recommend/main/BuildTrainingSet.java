package com.chinadaas.recommend.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.IDataAdapter;
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
import java.util.Map;

/**
 * Created by hejianning on 2017/9/29.
 * 根据正负例样本数据从 ES 中抽取特征,构建训练集,然后保存到 HDFS
 *
 *
 * 1	地区分布	    EE001	    省份	        全国热力图	百分比
 *      散列值       31个省份(不包括港澳台)
 *              参考 国家统计局行政区划代码到省、直辖市、自治区级别，行政区划代码取前两位，后四位为‘0000’
 *              110000北京市
 *              120000天津市
 *              130000河北省
 *              。。。。。。
 * 2	地区特征	    EB001	    地区特征	    柱状图	    百分比
 *      散列值     5个分类
 *              01一类城市
 *              02二类城市
 *              03三类城市
 *              04四类城市
 *              99不详
 * 3	地区级别	    EB002	    地区级别	    柱状图	    百分比
 *      散列值     4个分类
 * 　           01直辖市
 *              02省会城市
 *              03一般城市
 *              99不详
 * 4	行业排名	    EE011	    行业门类	    横柱状图	    百分比	按从高到低，显示前10
 *      散列值   20个分类
 *              国标行业门类代码
 * 5	行业特征	    EB003	    行业特征	    环形图	    百分比
 *      散列值     5个分类
 *              01高竞争行业（新增率高退出率高的行业）
 *              02新兴行业（新增率高退出率低的行业）
 *              03传统行业（新增率低退出率低的行业）
 *              04限制类行业(新增率低退出率高的行业)
 *              99不详　
 * 6	企业注册分布	EB004       企业生存年龄	折线图	    数量	    全库按年显示，新企库按月显示
 *      连续值
 *              取统计日期和成立日期月份差的最小整数。当成立日期为1900-01-01时生存年龄默认为空
 * 7	企业规模	    EB005	    企业规模	    饼状图	    百分比
 *      散列值     5个分类
 *              01大型
 *              02中型
 *              03小型
 *              04微型
 *              99不详
 * 8	企业类型	    EE007	    企业类型	    柱状图	    百分比	按照CA16级别1显示求和的百分比显示
 *      散列值     167个分类
 *              CA16原始码值
 */
public class BuildTrainingSet {

    static IDataAdapter ida = new EsDataAdapterImpl();

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("BuildTrainingSetApp")
                .enableHiveSupport()
                .getOrCreate();

        Map cfg = new HashMap<String,String>();

        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
//        cfg.put(DatabaseValues.ES_NODES,"192.168.207.14");
//        cfg.put(DatabaseValues.ES_PORT,"58200");
        cfg.put(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id","pripid");
        cfg.put("es.resource.read","ent_index/ENT_INDEX");

        /**
         * 从 HDFS获取正负例样本
         */
        JavaPairRDD<String, String> pncRDD = getPNC(spark);

        /**
         *  从 ES获取特征维度
         */
        JavaPairRDD featureRDD = getFeature(spark,cfg);

        /**
         * 得到训练数据
         */
        JavaPairRDD<String, Tuple2<String, String>> trainData = getTrainData(pncRDD, featureRDD);


        /**
         *训练集保存到 HDFS
         */
        String targetDir = "/tmp/spark_test/train_data";
        trainingDataArrange(spark,trainData,targetDir,false);

        spark.stop();

    }

    /**
     *
     * @param javaRDD
     * @param targetDir
     */
    public static void trainingDataArrange(SparkSession spark, JavaPairRDD<String, Tuple2<String, String>> javaRDD, String targetDir, final Boolean recommend){

        final Broadcast<Boolean> broadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(recommend);

        JavaRDD<String> mapRDD = javaRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
                        @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {

                return new Tuple2<>(stringTuple2Tuple2._2._1,stringTuple2Tuple2._2._2);
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                Boolean recommend = broadcast.value();
                String label = v1._1();
                if(recommend == true){
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
//        val hadoopConf = sparkContext.hadoopConfiguration
//        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//        if(hdfs.exists(path)){
//            //为防止误删，禁止递归删除
//            hdfs.delete(path,false)
//        }
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

    /**
     *
     * @param pncRDD
     * @param featureRDD
     * @return
     */
    public static JavaPairRDD<String, Tuple2<String, String>> getTrainData(JavaPairRDD<String,String> pncRDD, JavaPairRDD<String,String> featureRDD){
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
                if(split.length >= 7){
                    boo = true;
                }
                for(String str : split){
                    if(str == null || str.equals("") || str.equals("null") || str.equals("NULL") || str.equals("Null") || str.equals("!")){
                        boo = false;
                    }
                }
                return boo;
            }
        });

//        JavaRDD<String> pripidRDD = pairRDD.map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
//            @Override
//            public String call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
//
//                return stringTuple2Tuple2._1;
//            }
//        });
//
//        JavaPairRDD<String, String> featureRdd = pairRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
//
//                return new Tuple2<>(stringTuple2Tuple2._2._1,stringTuple2Tuple2._2._2);
//            }
//        });

//        if(savePripid == true){
//            if(!targetDir.equals("") && targetDir != null){
//                pripidRDD.saveAsTextFile(targetDir);
//            }else {
//                return null;
//            }
//        }

        return pairRDD;
    }

    /**
     *
     * @param spark
     * @param cfg
     * @return
     */
    public static JavaPairRDD getFeature(SparkSession spark, Map cfg){
        Dataset dataset = (Dataset)ida.loadData(spark, cfg);
        Dataset select = dataset.select("pripid","ee0001", "eb0001", "eb0002", "ee0011", "eb0003", "eb0004", "eb0005", "ee0007");
        JavaPairRDD<String,String> pairRDD = select.toJavaRDD().mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                String[] split = o.toString().replace("[", "").replace("]", "").split(",");
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 1; i < split.length; i++) {
                    stringBuilder.append(split[i]).append("|");
                }
                return new Tuple2(split[0], stringBuilder.toString().substring(0,stringBuilder.toString().length()-1));
            }
        });
//        List take = pairRDD.take(10000);
//        for(Object word : take){
//
//            System.out.println("特征维度    :   "+word);
//        }
        return pairRDD;
    }

    /**
     *
     * @param spark
     * @return
     */
    public static JavaPairRDD<String,String> getPNC(SparkSession spark){
        //RDD<String> rdd = spark.sparkContext().textFile("/tmp/spark_test/train_data",10);
        JavaRDD<String> rdd = spark.sparkContext().textFile("/tmp/spark_test/train_data_list.csv", 10).toJavaRDD();
        JavaPairRDD<String, String> javaPairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split("\t");
                return new Tuple2<>(split[1], split[12]);
            }
        });
//        JavaRDD<String> map = rdd.map(new Function<String, String>() {
//            @Override
//            public String call(String v1) throws Exception {
//                String[] split = v1.split("\t");
//                return (new StringBuilder().append(split[1]).append("\t").append(split[12]).toString());
//            }
//        });
//        JavaPairRDD<String, String> javaPairRDD = rdd.toJavaRDD().mapToPair(new PairFunction<String, String, String>() {
//            @Override
//            public Tuple2<String, String> call(String s) throws Exception {
//                String[] split = s.split("\t");
//                return new Tuple2<String, String>(split[1], split[0]);
//            }
//        });
//        Tuple2<String, String> first1 = javaPairRDD.first();
//        System.out.println("客户训练数据  : "+first1);
        return javaPairRDD;
    }


}
