package com.chinadaas.newEnt.etl.main;

import com.chinadaas.newEnt.etl.cfg.Constants;
import com.chinadaas.newEnt.etl.gaoDe.GaoDeUtils;
import com.chinadaas.newEnt.etl.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import com.google.common.base.Optional;

import java.util.*;

/**
 * Created by 74061 on 2017/6/21.
 */
public class New_Ent_BaseLocation_finStr {
    public static void main(String[] args) {

        //构建Spark运行时的环境参数
        SparkConf conf = new SparkConf().setAppName("New_Ent_BaseLocation").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new SQLContext(sc);

        //1、加载 hive表 NEW_ENT_BASEINFO 数据到dataFrame ，将 basedf 转换为 baseRdd
        DataFrame basedf = hc.sql("select PRIPID,DOM  from NEW_ENT_BASEINFO_MID where dom not in ('null','NULL') and dom is not null limit 10000 " );
        JavaRDD<Row> baseRdd = basedf.toJavaRDD();
        JavaPairRDD<String, String> baseDetailRDD = getBaseDetailRDD(baseRdd);
        //2、locationRDD ，从Oracle数据库中查询出数据 pripid,dom
        JavaPairRDD<String, String> locationRDD = getLocationRDD(hc);
        
        //7、取差集,这些值都要去查询location，然后插入location表
        JavaPairRDD<String,String> subRdd = baseDetailRDD.subtractByKey(locationRDD);
        
        //3、leftOuterJoin
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinRDD = locationRDD.leftOuterJoin(baseDetailRDD);

        //4、filter，留下不符合的，pripid相同，dom不同的数据,删除 Oracle 表中这些值，值应该不多
        JavaPairRDD<String, Tuple2<String, Optional<String>>> filtRdd = getFilterRdd(joinRDD);    

        //5、oracle ，delete NEW_ENT_BASEINFO_LOCATION_1
        deletFilterPripid(filtRdd);

        //6、mapToPair 整理格式
        JavaPairRDD<String, String> mapRdd = filtRdd.mapToPair(
                new PairFunction<Tuple2<String,Tuple2<String,Optional<String>>>, String, String>() {
                    private static final long serialVersionUID = 1L;
        
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<String>>> tu) throws Exception {
                        System.out.println("mapRdd"+tu._1+"----"+tu._2._1); 
                        
                        Optional<String> op = tu._2._2;
                        if(op.isPresent()){
                            return new Tuple2<String, String>(tu._1,op.get());
                        }
                        //这句话不会被执行
                        return null;
                    }
        });

        //8、取并集
        JavaPairRDD<String,String> unionRdd = mapRdd.union(subRdd);
        
        //9、插入oracle，对unionRdd进行操作,链接location，插入Oracle
        unionRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Iterator<Tuple2<String, String>> rows) throws Exception {

                List<String> list = new ArrayList<String>();

                while (rows.hasNext()) {
                    Tuple2<String, String> tu = rows.next();
                    String pripid = tu._1;
                    if(null ==pripid || "".equals(pripid)){
                        
                    }else{
                        String dom  =  tu._2;
                        System.out.println("unionRdd-======"+pripid+"---------"+dom);
                        list.add(pripid+" "+dom) ;
                    }
                }

                JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                String sql = "insert into NEW_ENT_BASEINFO_LOCATION_1"
                        + " (PRIPID, DOM, FORMATTED_ADDRESS, PROVINCE, CITY, "
                        + "  CITY_CODE, DISTRICT, ADCODE, LOCATION, LEV)  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                List<Object[]> params = new ArrayList<>();
                
                List<Map<String, String>> subList = new ArrayList<Map<String, String>>();
                int submitC = 6;
                for (int i = 0; i < list.size(); i++) {
                    String pripid = list.get(i).split(" ")[0];
                    String dom = list.get(i).split(" ")[1];
                    
                    Map<String, String> map = new HashMap<String,String>();
                    map.put("PRIPID",pripid);
                    map.put("DOM",dom);
                    subList.add(map);

                    if((i==list.size()-1) && ((i+1)%submitC !=0)){//有剩余
                        System.out.println("enter end");
                        GaoDeUtils.getLocation(subList,params);
                    }else{
                        if((i+1)%submitC==0){
                            System.out.println("enter 2");
                            GaoDeUtils.getLocation(subList,params);
                            subList.clear();
                        }
                    }
                }

                jdbcHelper.executeBatch(sql , params);
            }
        });

        sc.stop();
    }

    private static void deletFilterPripid(JavaPairRDD<String, Tuple2<String, Optional<String>>> filtRdd) {
        filtRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Optional<String>>>>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> rows) throws Exception {
                JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                
                String sql = "delete from NEW_ENT_BASEINFO_LOCATION_1 where pripid = ?";
                List<Object[]> params = new ArrayList<>();

                while (rows.hasNext()) {
                    Tuple2<String, Tuple2<String, Optional<String>>> t = rows.next();
                    String pripid= t._1;
                    System.out.println("delete  pripid===="+pripid);
                    params.add(new Object[]{pripid});
                }
                jdbcHelper.executeBatch(sql, params);
            }
        });

    }

    private static JavaPairRDD<String, Tuple2<String, Optional<String>>> getFilterRdd(JavaPairRDD<String, Tuple2<String, Optional<String>>> joinRDD) {
       
        JavaPairRDD<String, Tuple2<String, Optional<String>>> filtRdd = joinRDD.filter(
                new Function<Tuple2<String, Tuple2<String, Optional<String>>>, Boolean>() {
                    private static final long serialVersionUID = -2131762195257207144L;
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<String>>> tu) throws Exception {
                        String r1 = tu._2._1;
                        Optional<String> op = tu._2._2;

                        if(op.isPresent()){//有值
                            String r2 = tu._2._2.get();
                            String dom1 = String.valueOf(r1);
                            String dom2 = String.valueOf(r2);
                            System.out.println("dom1=:"+dom1+"-----dom2=:"+dom2);
                            if(dom1.equals(dom2)){
                                return false;
                            }else{                       
                                return true;
                            }
                        }else{//location中有值，base中无值，这种情况不应该有
                            return true;
                        }
                    }
        });
        return filtRdd;
    }

    private static JavaPairRDD<String,String> getLocationRDD(SQLContext hc) {
       
        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", Constants.JDBC_DRIVER);//oracle.jdbc.driver.OracleDriver
        options.put("url", Constants.JDBC_URL);
        options.put("dbtable", "new_ent_baseinfo_location_1");  //表
        options.put("user", Constants.JDBC_USER);
        options.put("password", Constants.JDBC_PASSWORD);
        // options.put("numPartitions", "4");
        //options.put("fetchsize", "1000");

        // 通过SQLContext去从MySQL中查询数据
        DataFrame areaInfoDF = hc.read().format("jdbc").options(options).load();
        JavaRDD<Row> areaInfoRDD = areaInfoDF.javaRDD();// 返回RDD

        JavaPairRDD<String, String> locationRdd = areaInfoRDD.mapToPair(
                new PairFunction<Row, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String pripid = String.valueOf(row.get(0));
                        String dom = String.valueOf(row.get(1));
                        return new Tuple2<String, String>(pripid, dom);
                    }
                });
        return locationRdd;
    }
    private static JavaPairRDD<String, String> getBaseDetailRDD(JavaRDD<Row> baseRDD) {
        //变为kv格式 ，输入 row ，返回key String value row  输入一个row，变为String和Row的KV格式
        JavaPairRDD<String, String> baseDetail = baseRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String pripid = row.getString(0);
                String dom = row.getString(1);
                return new Tuple2<String, String>(pripid,dom);
            }
        });
        return baseDetail;
    }
}