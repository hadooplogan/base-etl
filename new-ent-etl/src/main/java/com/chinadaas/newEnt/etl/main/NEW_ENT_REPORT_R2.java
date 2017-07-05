package com.chinadaas.newEnt.etl.main;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.chinadaas.newEnt.etl.cfg.Constants;
import com.chinadaas.newEnt.etl.table.DataQuality;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;


public class NEW_ENT_REPORT_R2 {
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setAppName("NEW_ENT_REPORT_R2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(sc);
       
        // 方法2、分别将oracle中数据加载为DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", Constants.JDBC_DRIVER);//oracle.jdbc.driver.OracleDriver
        options.put("url", Constants.JDBC_URL);
        options.put("user", Constants.JDBC_USER);
        options.put("password", Constants.JDBC_PASSWORD);
        options.put("numPartitions", "200");
        options.put("fetchsize", "1000");
        
        //行业码表
        options.put("dbtable", "T_DEX_APP_CODELIST_VIEW"); //行业
        DataFrame indDF = sqlc.read().format("jdbc").options(options).load();
        indDF.registerTempTable("T_DEX_APP_CODELIST_VIEW");
        String indSql = "select CODEVALUE,CODENAME FROM T_DEX_APP_CODELIST_VIEW where CODE_TYPE_VALUE = 'CA06' AND length(CODEVALUE) = 1";
        DataFrame indSqldf = sqlc.sql(indSql);
        JavaRDD<Row> indSqlRDD = indSqldf.javaRDD();
        List<Tuple2<String, String>> collect = indSqlRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                 
                String code = String.valueOf(row.get(0));
                String value = String.valueOf(row.get(1));
                return new Tuple2<String, String>(code,value) ;
            }
        }).collect();
        Map<String, String> indsqlMap = new HashMap<>();
        for(Tuple2<String, String> tu : collect){
            System.out.println(tu._1+"--------"+tu._2);
            indsqlMap.put(tu._1, tu._2);
        }
        
        //小类
        Map<String, String> indsql2Map = new HashMap<>();
        String indSql2 = "select substring(CODEVALUE,2),CODENAME FROM T_DEX_APP_CODELIST_VIEW where CODE_TYPE_VALUE = 'CA06' AND length(CODEVALUE) != 1";
        DataFrame indSqldf2 = sqlc.sql(indSql2);        
        JavaRDD<Row> indSql2RDD = indSqldf2.javaRDD();
        List<Tuple2<String, String>> collect2 = indSql2RDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                 
                String code = String.valueOf(row.get(0));
                String value = String.valueOf(row.get(1));
                return new Tuple2<String, String>(code,value) ;
            }
        }).collect();
        
        for(Tuple2<String, String> tu : collect2){
            indsql2Map.put(tu._1, tu._2);
        }
        final Broadcast<Map<String, String>> indsqlBroadCast = sc.broadcast(indsqlMap);
        final Broadcast<Map<String, String>> indsql2BroadCast = sc.broadcast(indsql2Map);
        
        ///////////////
        options.put("dbtable", "NEW_ENT_BASEINFO_20170704");
        DataFrame baseDF = sqlc.read().format("jdbc").options(options).load();  
        baseDF.registerTempTable("NEW_ENT_BASEINFO");
        
        
        //[PRIPID: string, string, REGNO: string, ENTTYPE: string, PPRIPID: string, PENTNAME: string, PREGNO: string, HYPOTAXIS: string, INDUSTRYPHY: string, INDUSTRYCO: string, ABUITEM: string, CBUITEM: string, OPFROM: timestamp, OPTO: timestamp, POSTALCODE: string, TEL: string, EMAIL: string, LOCALADM: string, CREDLEVEL: string, ASSDATE: timestamp, ESDATE: timestamp, APPRDATE: timestamp, REGORG: string, ENTCAT: string, ENTSTATUS: string, REGCAP: decimal(26,6), OPSCOPE: string, OPFORM: string, OPSCOANDFORM: string, PTBUSSCOPE: string, DOMDISTRICT: string, DOM: string, ECOTECDEVZONE: string, DOMPRORIGHT: string, OPLOCDISTRICT: string, OPLOC: string, RECCAP: decimal(26,6), INSFORM: string, PARNUM: decimal(10,0), PARFORM: string, EXENUM: decimal(20,0), EMPNUM: decimal(10,0), SCONFORM: string, FORCAPINDCODE: string, MIDPREINDCODE: string, PROTYPE: string, CONGRO: decimal(26,6), CONGROCUR: string, CONGROUSD: decimal(26,6), REGCAPUSD: decimal(26,6), REGCAPCUR: string, REGCAPRMB: decimal(26,6), FORREGCAPCUR: string, FORREGCAPUSD: decimal(26,6), FORRECCAPUSD: decimal(26,6), WORCAP: decimal(26,6), CHAMECDATE: timestamp, OPRACTTYPE: string, FORENTNAME: string, DEPINCHA: string, COUNTRY: string, ITEMOFOPORCPRO: string, CONOFCONTRPRO: string, FORDOM: string, FORREGECAP: decimal(26,6), FOROPSCOPE: string, S_EXT_ENTPROPERTY: string, S_EXT_TIMESTAMP: timestamp, S_EXT_BATCH: string, S_EXT_SEQUENCE: string, S_EXT_VALIDFLAG: string, S_EXT_INDUSCAT: string, S_EXT_ENTTYPE: string, MANACATE: string, LIMPARNUM: decimal(10,0), FOREIGNBODYTYPE: string, PERSON_ID: string, NAME: string, CERTYPE: string, CERNO: string, ANCHEYEAR: string, CANDATE: timestamp, REVDATE: timestamp, ENTNAME_OLD: string, CREDIT_CODE: string, JOBID: string, IS_NEW: decimal(38,10), COUNTRYDISPLAY: string, STATUSDISPLAY: string, TYPEDISPLAY: string, REGORGDISPLAY: string, REGCAPCURDISPLAY: string, ENTID: string, HANDLE_TYPE: decimal(2,0), TAX_CODE: string, LICID: string, ZSPID: string]      
        options.put("dbtable", "NEW_ENT_INVINFO_20170704"); //覆盖
        DataFrame invDF = sqlc.read().format("jdbc").options(options).load();//[S_EXT_NODENUM: string, PRIPID: string, INVID: string, INV: string, INVTYPE: string, CERTYPE: string, CERNO: string, BLICTYPE: string, BLICNO: string, COUNTRY: string, CURRENCY: string, SUBCONAM: decimal(18,6), ACCONAM: decimal(18,6), SUBCONAMUSD: decimal(18,6), ACCONAMUSD: decimal(18,6), CONPROP: decimal(26,6), CONFORM: string, CONDATE: timestamp, BALDELPER: timestamp, CONAM: decimal(18,6), EXEAFFSIGN: string, S_EXT_TIMESTAMP: timestamp, S_EXT_BATCH: string, S_EXT_SEQUENCE: string, S_EXT_VALIDFLAG: string, LINKMAN: string, JOBID: string, ENT_ID: string, TYPEDISPLAY: string, HANDLE_TYPE: decimal(2,0), ZSPID: string]
        invDF.registerTempTable("NEW_ENT_INVINFO");        
        options.put("dbtable", "new_ent_baseinfo_location"); //覆盖
        DataFrame locDF = sqlc.read().format("jdbc").options(options).load();//[PRIPID: string, DOM: string, FORMATTED_ADDRESS: string, PROVINCE: string, CITY: string, CITY_CODE: string, DISTRICT: string, ADCODE: string, LOCATION: string, LEV: string]
        locDF.registerTempTable("NEW_ENT_BASEINFO_location");
        
        String sql = " select b.PRIPID,b.ESDATE ,b.S_EXT_SEQUENCE,b.REGNO,b.INDUSTRYPHY,"
                   + "        b.INDUSTRYCO,b.TEL,b.NAME,b.DOM,i.INV,l.LOCATION,b.CREDIT_CODE,b.REGORG"
                   + " from NEW_ENT_BASEINFO b left outer join NEW_ENT_INVINFO  i "
                   + " on b.PRIPID = i.PRIPID left outer join NEW_ENT_BASEINFO_location l "
                   + " on b.PRIPID = l.PRIPID  where b.REGORG is not null and  i.INV is null ";
        
        DataFrame sql2 = sqlc.sql(sql);
        JavaRDD<Row> infoRDD = sql2.javaRDD();// 返回RDD
        
        //遍历每条数据，判断每个字段是否有效
        JavaPairRDD<String, DataQuality> mapToPair = infoRDD.mapToPair(
            new PairFunction<Row, String, DataQuality>() {
                private static final long serialVersionUID = 1L;
    
                @Override
                public Tuple2<String, DataQuality> call(Row row) throws Exception {
                    Map<String, String> indsqlMap = indsqlBroadCast.value();
                    Map<String, String> indsql2Map = indsql2BroadCast.value();
                    
                    String pripid = String.valueOf(row.getAs("PRIPID"));
                    String regorg = String.valueOf(row.getAs("REGORG"));//行政区域编号
                    String seq = String.valueOf(row.getAs("S_EXT_SEQUENCE"));
                    String esdate = String.valueOf(row.getAs("ESDATE"));
                    String key="";
                    System.out.println(pripid+"====="+regorg+"========"+seq+"======"+esdate);
                    if(regorg==null || "null".equals(regorg)|| "".equals(regorg )){
                        key = esdate.substring(0,10).replace("-", "")+"_"+seq.substring(0,8);
                    }else{
                        key = regorg.substring(0,6)+"_"+esdate.substring(0,10).replace("-", "")+"_"+seq.substring(0,8);
                    }
                    System.out.println(key);
                   
                    //注册号为空 ，无效
                    Integer regnoInvalid=0;
                    String regno = row.getAs("REGNO")==null?"": String.valueOf(row.getAs("REGNO"));
                    if("".equals(regno)){
                        regnoInvalid=1;//1 无效
                    }
                    
                    //统一信用代码为空，无效
                    Integer creditCodeInvalid =0;
                    String creditCode = row.getAs("CREDIT_CODE")==null?"":String.valueOf(row.getAs("CREDIT_CODE"));
                    if("".equals(creditCode)){
                        creditCodeInvalid=1;//1 无效
                    }
                   
                    //tel为null或不符合正则规则，无效
                    Integer telInvalid=0;
                    String tel = row.getAs("TEL")==null?"": String.valueOf(row.getAs("TEL"));
                    if("".equals(tel)){
                        telInvalid=1;//1 无效
                    }
                    /* boolean isTel = GeneralUtils.regx(tel);
                    if(!isTel){
                        tel="";
                    }*/
                    
                    //法人为空，无效
                    Integer nameInvalid = 0;
                    String name = row.getAs("NAME")==null?"":String.valueOf(row.getAs("NAME"));
                    if("".equals(name)){
                        nameInvalid=1;//1 无效
                    }
                    
                    //为空 或不在范围内
                    Integer industryphyInvalid =0;
                    String industryphy = row.getAs("INDUSTRYPHY")==null?"":String.valueOf(row.getAs("INDUSTRYPHY"));
                    if(!indsqlMap.containsKey(industryphy)){
                        industryphy="";
                    }
                    if("".equals(industryphy)){
                        industryphyInvalid=1;//1 无效
                    }
                    
                     //为空 或不在范围内
                    Integer industrycoInvalid = 0;
                    String industryco  = row.getAs("INDUSTRYCO")==null?"":String.valueOf(row.getAs("INDUSTRYCO"));
                    if(!indsql2Map.containsKey(industryco)){
                        industryco="";
                    }
                    if("".equals(industryco)){
                        industrycoInvalid=1;//1 无效
                    }
                    //高德无法定位
                    Integer domInvalid =0;
                    String dom = row.getAs("DOM")==null?"":String.valueOf(row.getAs("DOM"));
                    if("".equals(dom)){
                        domInvalid=1;//1 无效
                    }
                    System.out.println(regno+"-----------"+dom);
                    
                    //股东名称为空，无效，一个pripid，可能有多个股东
                    Integer invInvalid =0;
                    String inv = String.valueOf(row.getAs("INV"));
                    if("".equals(inv)){
                        invInvalid=1;//1 无效
                    }
                    
                    DataQuality dq = new DataQuality(regnoInvalid,creditCodeInvalid,telInvalid,
                            nameInvalid,industryphyInvalid,industrycoInvalid,domInvalid,invInvalid);
                    return new Tuple2<String, DataQuality>(key,dq);
                }
        })/*.partitionBy(new Partitioner() {
            private static final long serialVersionUID = 1L;

            @Override
            public int numPartitions() {
                return 200;
            }
            
            @Override
            public int getPartition(Object obj) {
                return obj.hashCode() % numPartitions();
            }
        })*/;
        
        
        mapToPair.reduceByKey(new Function2<DataQuality, DataQuality, DataQuality>() {
            private static final long serialVersionUID = 1L;

            @Override
            public DataQuality call(DataQuality dq1, DataQuality dq2) throws Exception {
                Integer regnoC = dq1.getRegnoInvalid()+dq2.getRegnoInvalid();
                Integer creditCodeC = dq1.getCreditCodeInvalid()+dq2.getCreditCodeInvalid();
                Integer telC = dq1.getTelInvalid()+dq2.getTelInvalid();
                Integer nameC = dq1.getNameInvalid()+dq2.getNameInvalid();
                Integer industryphyC = dq1.getIndustryphyInvalid()+dq2.getIndustryphyInvalid();
                Integer industrycoC = dq1.getIndustrycoInvalid()+dq2.getIndustrycoInvalid();
                Integer domC = dq1.getDomInvalid()+dq2.getDomInvalid();
                Integer invC  = dq1.getInvInvalid()+dq2.getInvInvalid();
                System.out.println(regnoC+"-----------");
                DataQuality dq = new DataQuality(regnoC,creditCodeC,telC,nameC,industryphyC,industrycoC,domC,invC);
                
                return dq;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String,DataQuality>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, DataQuality>> iter) throws Exception {
                Class.forName(Constants.JDBC_DRIVER);//加载驱动
                Connection conn = null;
                PreparedStatement st = null;
                
                String sql ="insert into new_ent_report_r2 ( "
                        + " regorg,esdate,storeTime,regno,creditCode,tel,name,industryphy,industryco,dom) "
                        + " values (?,?,?,?,?,?,?,?,?,? )";

                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.205.31:1521:daas31", "BI", "BI");
                    conn.setAutoCommit(false);

                    st = conn.prepareStatement(sql);
                    while (iter.hasNext()) {
                        Tuple2<String, DataQuality> t = iter.next();
                        String keys = t._1;
                        DataQuality dq = t._2;
                        
                        
                        if(keys.split("_").length>2){
                            st.setString(1, keys.split("_")[0]);
                            st.setString(2, keys.split("_")[1]);
                            st.setString(3, keys.split("_")[2]);
                        }else{
                            st.setString(1,"");
                            st.setString(2, keys.split("_")[0]);
                            st.setString(3, keys.split("_")[1]);
                        }
                     
                        st.setInt(4, dq.getRegnoInvalid());
                        st.setInt(5, dq.getCreditCodeInvalid());
                        st.setInt(5, dq.getTelInvalid());
                        st.setInt(6, dq.getTelInvalid());
                        st.setInt(7, dq.getNameInvalid());
                        st.setInt(8, dq.getIndustryphyInvalid());
                        st.setInt(9, dq.getIndustrycoInvalid());
                        st.setInt(10, dq.getDomInvalid());
                        st.addBatch();
                    }
                    st.executeBatch();
                    conn.commit();
                    st.clearBatch();
                } catch (Exception e) {
                    if (conn != null) {
                        conn.rollback();
                    }
                    throw(e);
                }finally{
                    if (st != null) {
                        st.close();
                        st = null;
                    }
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                }
            }
        });
        
        sc.close();//将SparkContext 关闭，释放资源
        
    }

}
