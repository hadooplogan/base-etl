package com.chinadaas.recommend.main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hejianning on 2017/10/13.
 *
 * 根据训练集训练模型,然后把模型保存到 HDFS
 *
 *
 * 1	地区分布	    EE001	    省份	        全国热力图	百分比
 * 散列值       31个省份(不包括港澳台)
 * 参考 国家统计局行政区划代码到省、直辖市、自治区级别，行政区划代码取前两位，后四位为‘0000’
 * 110000北京市
 * 120000天津市
 * 130000河北省
 * 。。。。。。
 * 2	地区特征	    EB001	    地区特征	    柱状图	    百分比
 * 散列值     5个分类
 * 01一类城市
 * 02二类城市
 * 03三类城市
 * 04四类城市
 * 99不详
 * 3	地区级别	    EB002	    地区级别	    柱状图	    百分比
 * 散列值     4个分类
 * 　           01直辖市
 * 02省会城市
 * 03一般城市
 * 99不详
 * 4	行业排名	    EE011	    行业门类	    横柱状图	    百分比	按从高到低，显示前10
 * 散列值   20个分类
 * 国标行业门类代码
 * 5	行业特征	    EB003	    行业特征	    环形图	    百分比
 * 散列值     5个分类
 * 01高竞争行业（新增率高退出率高的行业）
 * 02新兴行业（新增率高退出率低的行业）
 * 03传统行业（新增率低退出率低的行业）
 * 04限制类行业(新增率低退出率高的行业)
 * 99不详　
 * 6	企业规模	    EB005	    企业规模	    饼状图	    百分比
 * 散列值     5个分类
 * 01大型
 * 02中型
 * 03小型
 * 04微型
 * 99不详
 * 7	企业类型	    EE007	    企业类型	    柱状图	    百分比	按照CA16级别1显示求和的百分比显示
 * 散列值     167个分类
 * CA16原始码值
 * 8	企业注册分布	EB004       企业生存年龄	折线图	    数量	    全库按年显示，新企库按月显示
 * 连续值
 * 取统计日期和成立日期月份差的最小整数。当成立日期为1900-01-01时生存年龄默认为空
 *
 */
public class ClassificationDecisionTree {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTreeClassification");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
        String datapath = "/tmp/spark_test/test_data";
        //String datapath = "/tmp/spark_test/train_data";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.5, 0.5});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

// Set parameters.一个映射表，用来指定那些特征是分类的，以及他们各有多少个分类。多写一个分类，是分给空值。
//  Empty categoricalFeaturesInfo indicates all features are continuous.空的分类特征信息指示所有特征是连续的。
        Integer numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        categoricalFeaturesInfo.put(1,6);
        categoricalFeaturesInfo.put(2,5);
        categoricalFeaturesInfo.put(3,21);
        categoricalFeaturesInfo.put(4,6);
        categoricalFeaturesInfo.put(5,6);
       // String impurity = "entropy";
        String impurity = "gini";
        Integer maxDepth = 20;
        Integer maxBins = 32;

// Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);
        // evalute model on train instances and compute train test error

        JavaPairRDD<Double, Double> predictlabel = trainingData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
            @Override
            public Tuple2<Double, Double> call(LabeledPoint labeledPoint) throws Exception {
                return new Tuple2<>(model.predict(labeledPoint.features()), labeledPoint.label());
            }
        });

        Double trainErr = 1.0 * predictlabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> v1) throws Exception {
                return !v1._1().equals(v1._2());
            }
        }).count() / testData.count();
        System.out.println("Test Erro:"+trainErr);


// Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();

        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification tree model:\n" + model.toDebugString());

// Save and load model
        model.save(jsc.sc(), "/tmp/spark_test/20");
//        DecisionTreeModel sameModel = DecisionTreeModel
//                .load(jsc.sc(), "/tmp/spark_test/modelData");

    }
}
