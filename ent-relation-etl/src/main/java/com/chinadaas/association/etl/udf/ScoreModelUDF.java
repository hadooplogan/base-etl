package com.chinadaas.association.etl.udf;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

/**
 * Created by gongxs01 on 2017/6/13.
 */
public class ScoreModelUDF {

    private  static double hold = 0.67;
    private  static double rehold = 0.51;
    private  static double safehold = 0.34;
    private  static double joinhold = 0.01;

    private static String[] staffs = {"410A","410B","410C","410D","410E","410F","410G","410Z","430A","431A","432K","433A","433B","434Q","436A","441A","441B","441C","441D","441E","441F","441G","442G","451D","490A","491A"};

    public static void riskScore(SparkContext sc, HiveContext sqlContext) {
        sqlContext.udf().register("riskscore", new UDF2<Integer, String,Double>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;

            public Double call(Integer type ,String ent) {
                return getRiskScore(type,ent);
            }
        }, DataTypes.DoubleType);
    }


    /**
     *type=1 为企业投资或者自然人投资
     *type=2 为任职信息
     *type=3 为疑似信息
     *
     *ent格式 entstatus(企业状态)|enttype(企业类型)|占比信息或者职位信息或者
     *
     */
    public static Double getRiskScore(Integer type ,String ent) {
        String[] entInfo = ent.split("\\|");
        Double score = 0.0;
        if(entInfo.length!=3){
            return 0.0;
        }
        if(entInfo[0]==null||"".equals(entInfo[0])){
            return 0.0;
        }
        if(entInfo[1]==null||"".equals(entInfo[1])){
            return 0.0;
        }
        if(entInfo[2]==null||"".equals(entInfo[2])){
            return 0.0;
        }

        //有限合伙企业（4533）/注销、吊销
        if("4533".equals(entInfo[1])||"2".equals(entInfo[0])||"3".equals(entInfo[0])){
            return 0.0;
        }

        switch(type)
        {
            case 1:
                return converScore(entInfo[2]);
            case 2:
                for(String staff:staffs){
                    if(staff.equals(entInfo[2])){
                        return 1.0;
                    }
                }
                return 0.1;
            case 3:;
            default:
            break;
        }
       return 0.0;
    }

    private static double converScore(String entInfo){
        if(Double.valueOf(entInfo)>=hold) return 1.0;
        if(Double.valueOf(entInfo)<hold&&Double.valueOf(entInfo)>=rehold) return 0.8;
        if(Double.valueOf(entInfo)<rehold&&Double.valueOf(entInfo)>=safehold) return 0.5;
        if(Double.valueOf(entInfo)<safehold&&Double.valueOf(entInfo)>=joinhold) return 0.3;
        if(Double.valueOf(entInfo)<joinhold) return 0.1;

        return 0.0;
    }

    public static void main(String[] args){
       //   System.out.println(getRiskScore(1,"1||0.68"));
       // System.out.println(getRiskScore(1,"1|123|0.005"));
       // System.out.println(getRiskScore(2,"1|231|410BB"));
    }
}
