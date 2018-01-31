package com.chinadaas.association.etl.udf;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
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

    private static String[] staffs = {"410A","410B","410C","410D","408E","410E","410F","410G","432A","410Z","430A","431A","432K","433A","433B","434Q","436A","441A","441B","441C","441D","441E","441F","441G","442G","451D","490A","491A","451C"};
//    private static String[] staffs = {
//        "410A","410B","410C","410D","410E","410F","410G","410Z","430A","431A","431B","432A","432K","433A","433B","434Q","441A","441B","441C","441D","441E","441F","441G","442G","451C","451D","451E","490A","491A","493A","A004","A005","A015","A016","A017","A019","A022","A023","A025","A027","A029","A030","A033","A034","A041","A042","A043","A044","A045","A046","A047","A048","A049","A050","A051","A052","A053","A057","A139","A140","A141","A143","A147","A149","A150","A152","A153","A154","A155","A156","A157","A158","A160","A161","A162","A163","A165","A169","A170","A175","A176","A186","A187","A188","A189","A190","A191","A192","A193","A197","A198","A199","A200","A201","A204","A210","A222","A236","A237","A242"
//};

    public static void riskScore(SparkSession spark) {
        spark.udf().register("riskscore", new UDF2<Integer, String,Double>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;

            @Override
            public Double call(Integer type , String ent) {
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
        if("0000".equals(entInfo[1])||"2".equals(entInfo[0])||"3".equals(entInfo[0])||"21".equals(entInfo[0])||"22".equals(entInfo[0])){
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
