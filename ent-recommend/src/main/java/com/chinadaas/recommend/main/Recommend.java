package com.chinadaas.recommend.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.recommend.entity.NewEntBaseInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 潜客推荐:根据新企做推荐
 */
public class Recommend {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("RecommendApp")
                .enableHiveSupport()
                .getOrCreate();

        String path = args[0];
        System.out.println("正例数据    :   "+path);

        Map cfg = new HashMap<String,String>();

        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id","pripid");
        cfg.put("es.resource.read","ent_index/ENT_INDEX");

        /**
         * 从 Oracle获得新企数据
         */
        JavaRDD baseRDD = getRecommendBase(spark);

        /**
         * 构建分类数据
         * (pripid,pripid),让label为 pripid, 分类后可以通过 pripid 得到具体的企业信息
         */
        JavaPairRDD<String, String> classificationData = getRecommend(baseRDD);

        /**
         *  从 ES获取特征维度
         */
        JavaPairRDD<String,String> featureRDD = BuildTrainingSet.getFeature(spark, cfg);

        /**
         * 得到推荐数据
         */
//        String pripirdRddDir = "/tmp/spark_test/pripid_data";
        JavaPairRDD<String, Tuple2<String, String>> trainData = BuildTrainingSet.getTrainData(classificationData, featureRDD);

        /**
         *  把推荐数据保存到 HDFS 目录
         */

        String targetDir = "/tmp/spark_test/recommend_data";
        BuildTrainingSet.trainingDataArrange(spark, trainData, targetDir, true);

        /**
         * 用现有决策树模型进行分类,label为1的做为正例,把正例推荐给客户
         */
        JavaPairRDD<String,String> recommend = recommend(spark, targetDir, trainData);

        /**
         * 用pripid从新企数据中得到推荐的企业的信息然后存到HDFS目录
         * /home/spark/spark-2.1.1/bin/spark-submit --executor-memory 20g --num-executors 20 --executor-cores 10 --class com.chinadaas.association.etl.main.Recommend --master yarn /home/spark/ent-relation-etl-1.0-SNAPSHOT-jar-with-dependencies.jar
         */
        //存储路径需要修改，可以按照客户的uuid进行创建新的文件夹，然后存储 part-0000这样的文件
        String entDir = "/tmp/spark_test/recommend_test";
        saveRecommendData(spark,recommend,baseRDD,entDir);

    }

    public static void saveRecommendData(SparkSession spark, JavaPairRDD<String,String> recommend, JavaRDD baseRDD,String entDir){
        JavaPairRDD<String,NewEntBaseInfo> pairRDD = baseRDD.mapToPair(new PairFunction<NewEntBaseInfo, String, NewEntBaseInfo>() {
            @Override
            public Tuple2<String, NewEntBaseInfo> call(NewEntBaseInfo entBaseInfo) throws Exception {
                String pripid = entBaseInfo.getPripid().replace("[", "").replace("]", "");
                return new Tuple2<>(pripid, entBaseInfo);
            }
        });
        JavaRDD<NewEntBaseInfo> newEntBaseInfoJavaRDD = recommend.join(pairRDD).map(new Function<Tuple2<String, Tuple2<String, NewEntBaseInfo>>, NewEntBaseInfo>() {
            @Override
            public NewEntBaseInfo call(Tuple2<String, Tuple2<String, NewEntBaseInfo>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._2._2;
            }
        });


//        NewEntBaseInfo first = newEntBaseInfoJavaRDD.first();
//        System.out.println(first);
        Path path = null;
        Configuration entries = null;
        FileSystem fileSystem = null;
        try {
            path = new Path(entDir);
            entries = spark.sparkContext().hadoopConfiguration();
            fileSystem = FileSystem.get(entries);
            if(fileSystem.exists(path)){
                //递归删除目录
                fileSystem.delete(path,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        newEntBaseInfoJavaRDD.saveAsTextFile(entDir);
    }

    public static JavaPairRDD<String,String> recommend(SparkSession spark, String targetDir, JavaPairRDD<String, Tuple2<String, String>> trainData){
       // final DecisionTreeModel model = DecisionTreeModel
        //        .load(spark.sparkContext(), "/tmp/spark_test/modelData");
     final DecisionTreeModel model = DecisionTreeModel.load(spark.sparkContext(), "/tmp/spark_test/new_test_model");
        JavaPairRDD<String, Integer> labelRDD = MLUtils.loadLibSVMFile(spark.sparkContext(), targetDir)
                .toJavaRDD()
                .map(new Function<LabeledPoint, Integer>() {
                    @Override
                    public Integer call(LabeledPoint p) throws Exception {

                        return (int) model.predict(p.features());
                    }
                })
                .zipWithIndex()
                .mapToPair(new PairFunction<Tuple2<Integer, Long>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, Long> integerLongTuple2) throws Exception {
                        return new Tuple2<>(String.valueOf(integerLongTuple2._2), integerLongTuple2._1);
                    }
                });

        //获得pripidRDD
        JavaPairRDD<String, String> pripidRDD = trainData
                .map(new Function<Tuple2<String,Tuple2<String,String>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        return stringTuple2Tuple2._1;
                    }
                })
                .zipWithIndex()
                .mapToPair(new PairFunction<Tuple2<String, Long>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return new Tuple2<>(String.valueOf(stringLongTuple2._2), stringLongTuple2._1);
                    }
                });

            spark.log().debug("开始组合 pripid 和 label");

        JavaPairRDD<String,String> pripid = labelRDD.join(pripidRDD)
                .filter(new Function<Tuple2<String, Tuple2<Integer, String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<Integer, String>> stringTuple2Tuple2) throws Exception {
                        return stringTuple2Tuple2._2._1 == 1;
                    }
                })
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, String, String>() {
                    @Override
                    public Tuple2<String,String> call(Tuple2<String, Tuple2<Integer, String>> stringTuple2Tuple2) throws Exception {
                        return new Tuple2<>(stringTuple2Tuple2._2._2,stringTuple2Tuple2._2._2);
                    }
                });
            return pripid;
    }

    public static JavaPairRDD<String,String> getRecommend(JavaRDD data){
        JavaPairRDD javaPairRDD = data.mapToPair(new PairFunction<NewEntBaseInfo, String, String>() {
            @Override
            public Tuple2<String, String> call(NewEntBaseInfo entBaseInfo) throws Exception {
                String pripid = entBaseInfo.getPripid().replace("[", "").replace("]", "");
                return new Tuple2<>(pripid, pripid);
            }
        });
        long count = javaPairRDD.count();
        System.out.println("count   :   "+count);
        return javaPairRDD;
    }
  //在这个位置可以 空值一下新企的推荐的公司。

    public static JavaRDD getRecommendBase(SparkSession spark){

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver","oracle.jdbc.driver.OracleDriver")
                .option("url", "jdbc:oracle:thin:NEWENT/NEWENT@//192.168.205.25:1521/bipdb")
                .option("dbtable", "NEW_ENT_BASEINFO")
//                .option("user", "NEWENT")
//                .option("password", "NEWENT")
                .load();



        JavaRDD<NewEntBaseInfo> map = jdbcDF.toJavaRDD().map(new Function<Row, NewEntBaseInfo>() {
            @Override
            public NewEntBaseInfo call(Row row) throws Exception {
                String[] split = row.toString().split(",");
                String pripid = "";
                if(!split[0].equals("") && split[0] != null){
                    pripid = split[0].toString();
                }
                String s_ext_nodenum = "";
                if(!split[1].equals("") && split[1] != null){
                    s_ext_nodenum = split[1].toString();
                }
                String entname = "";
                if(!split[2].equals("") && split[2] != null){
                    entname = split[2].toString();
                }
                String regno = "";
                if(!split[3].equals("") && split[3] != null){
                    regno = split[3].toString();
                }
                String enttype = "";
                if(!split[4].equals("") && split[4] != null){
                    enttype = split[4].toString();
                }
                String industryphy = "";
                if(!split[5].equals("") && split[5] != null){
                    industryphy = split[5].toString();
                }
                String industryco = "";
                if(!split[6].equals("") && split[6] != null){
                    industryco = split[6].toString();
                }
                String abuitem = "";
                if(!split[7].equals("") && split[7] != null){
                    abuitem = split[7].toString();
                }
                String cbuitem = "";
                if(!split[8].equals("") && split[8] != null){
                    cbuitem = split[8].toString();
                }
                String opfrom = "";
                if(!split[9].equals("") && split[9] != null){
                    opfrom = split[9].toString();
                }
                String opto = "";
                if(!split[10].equals("") && split[10] != null){
                    opto = split[10].toString();
                }
                String postalcode = "";
                if(!split[11].equals("") && split[11] != null){
                    postalcode = split[11].toString();
                }
                String tel = "";
                if(!split[12].equals("") && split[12] != null){
                    tel = split[12].toString();
                }
                String email = "";
                if(!split[13].equals("") && split[13] != null){
                    email = split[13].toString();
                }
                String credlevel = "";
                if(!split[14].equals("") && split[14] != null){
                    credlevel = split[14].toString();
                }
                String esdate = "";
                if(!split[15].equals("") && split[15] != null){
                    esdate = split[15].toString();
                }
                String apprdate = "";
                if(!split[16].equals("") && split[16] != null){
                    apprdate = split[16].toString();
                }
                String regorg = "";
                if(!split[17].equals("") && split[17] != null){
                    regorg = split[17].toString();
                }
                String entstatus = "";
                if(!split[18].equals("") && split[18] != null){
                    entstatus = split[18].toString();
                }
                String regcap = "";
                if(!split[19].equals("") && split[19] != null){
                    regcap = split[19].toString();
                }
                String opscope = "";
                if(!split[20].equals("") && split[20] != null){
                    opscope = split[20].toString();
                }
                String domdistrict = "";
                if(!split[21].equals("") && split[21] != null){
                    domdistrict = split[21].toString();
                }
                String dom = "";
                if(!split[22].equals("") && split[22] != null){
                    dom = split[22].toString();
                }
                String empnum = "";
                if(!split[23].equals("") && split[23] != null){
                    empnum = split[23].toString();
                }
                String regcapcur = "";
                if(!split[24].equals("") && split[24] != null){
                    regcapcur = split[24].toString();
                }
                String country = "";
                if(!split[25].equals("") && split[25] != null){
                    country = split[25].toString();
                }
                String s_ext_timestamp = "";
                if(!split[26].equals("") && split[26] != null){
                    s_ext_timestamp = split[26].toString();
                }
                String s_ext_sequence = "";
                if(!split[27].equals("") && split[27] != null){
                    s_ext_sequence = split[27].toString();
                }
                String person_id = "";
                if(!split[28].equals("") && split[28] != null){
                    person_id = split[28].toString();
                }
                String name = "";
                if(!split[29].equals("") && split[29] != null){
                    name = split[29].toString();
                }
                String certype = "";
                if(!split[30].equals("") && split[30] != null){
                    certype = split[30].toString();
                }
                String cerno = "";
                if(!split[31].equals("") && split[31] != null){
                    cerno = split[31].toString();
                }
                String candate = "";
                if(!split[32].equals("") && split[32] != null){
                    candate = split[32].toString();
                }
                String revdate = "";
                if(!split[33].equals("") && split[33] != null){
                    revdate = split[33].toString();
                }
                String entname_old = "";
                if(!split[34].equals("") && split[34] != null){
                    entname_old = split[34].toString();
                }
                String credit_code = "";
                if(!split[35].equals("") && split[35] != null){
                    credit_code = split[35].toString();
                }
                String jobid = "";
                if(!split[36].equals("") && split[36] != null){
                    jobid = split[36].toString();
                }
                String is_new = "";
                if(!split[37].equals("") && split[37] != null){
                    is_new = split[37].toString();
                }
                String countrydisplay = "";
                if(!split[38].equals("") && split[38] != null){
                    countrydisplay = split[38].toString();
                }
                String statusdisplay = "";
                if(!split[39].equals("") && split[39] != null){
                    statusdisplay = split[39].toString();
                }
                String typedisplay = "";
                if(!split[40].equals("") && split[40] != null){
                    typedisplay = split[40].toString();
                }
                String regorgdisplay = "";
                if(!split[41].equals("") && split[41] != null){
                    regorgdisplay = split[41].toString();
                }
                String regcapcurdisplay = "";
                if(!split[42].equals("") && split[42] != null){
                    regcapcurdisplay = split[42].toString();
                }
                String tax_code = "";
                if(!split[43].equals("") && split[43] != null){
                    tax_code = split[43].toString();
                }
                String licid = "";
                if(!split[44].equals("") && split[44] != null){
                    licid = split[44].toString();
                }
                String zspid = "";
                if(!split[45].equals("") && split[45] != null){
                    zspid = split[45].toString();
                }
                String data_date = "";
                if(!split[46].equals("") && split[46] != null){
                    data_date = split[46].toString();
                }
                String verify_flag = "";
                if(!split[47].equals("") && split[47] != null){
                    verify_flag = split[47].toString();
                }
                String altdate = "";
                if(!split[48].equals("") && split[48] != null){
                    altdate = split[48].toString();
                }
                String inv_nums = "";
                if(!split[49].equals("") && split[49] != null){
                    inv_nums = split[49].toString();
                }
                String person_nums = "";
                if(!split[50].equals("") && split[50] != null){
                    person_nums = split[50].toString();
                }
                String hk_value = "";
                if(!split[51].equals("") && split[51] != null){
                    hk_value = split[51].toString();
                }

                NewEntBaseInfo newEntBaseInfo = new NewEntBaseInfo(
                pripid,
                s_ext_nodenum,
                entname,
                regno,
                enttype,
                industryphy,
                industryco,
                abuitem,
                cbuitem,
                opfrom,
                opto,
                postalcode,
                tel,
                email,
                credlevel,
                esdate,
                apprdate,
                regorg,
                entstatus,
                regcap,
                opscope,
                domdistrict,
                dom,
                empnum,
                regcapcur,
                country,
                s_ext_timestamp,
                s_ext_sequence,
                person_id,
                name,
                certype,
                cerno,
                candate,
                revdate,
                entname_old,
                credit_code,
                jobid,
                is_new,
                countrydisplay,
                statusdisplay,
                typedisplay,
                regorgdisplay,
                regcapcurdisplay,
                tax_code,
                licid,
                zspid,
                data_date,
                verify_flag,
                altdate,
                inv_nums,
                person_nums,
                hk_value
                );
                return newEntBaseInfo;
            }
        });
//        long count = map.count();
//        System.out.println("count   :   "+count);
//        NewEntBaseInfo first = map.first();
//        System.out.println(first);
        return map;
    }

//    public static String getTargetString(Row row, String name){
//        String target = "";
//        if(!row.getAs(name).equals("") && row.getAs(name) != null){
//            row.getAs(name).toString();
//        }
//        return target;
//    }

}
