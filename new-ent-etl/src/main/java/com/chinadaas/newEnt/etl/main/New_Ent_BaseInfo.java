hgpackage com.chinadaas.newEnt.etl.main;

import com.chinadaas.newEnt.etl.cfg.Constants;
import com.chinadaas.newEnt.etl.general.GeneralUtils;
import com.chinadaas.newEnt.etl.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 74061 on 2017/6/13.
 */
public class New_Ent_BaseInfo {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_BaseInfo");
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

        /*SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");//设置日期格式
        final java.sql.Date batchDate = GeneralUtils.getColumnsDate(sdf.format(new Date()));*/
        final String currYearDate= GeneralUtils.getYearRangeDate().split("_")[0];
        //String lastYearDate= GeneralUtils.getYearRangeDate().split("_")[1];

        //在Oracle中新建表
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.createtable("drop table NEW_ENT_BASEINFO_"+currYearDate);
        jdbcHelper.createtable(createTableStr(currYearDate));

        //1、加载 hive表中数据到dataFrame
        DataFrame df = hc.sql("select  " +
                "base.PRIPID,base.S_EXT_NODENUM,base.ENTNAME,base.ORIREGNO,base.REGNO," +
                "base.ENTTYPE,base.PPRIPID,base.PENTNAME,base.PREGNO,base.HYPOTAXIS," +
                "base.INDUSTRYPHY,base.INDUSTRYCO,base.ABUITEM,base.CBUITEM,base.OPFROM," +
                "base.OPTO,base.POSTALCODE,base.TEL,base.EMAIL,base.LOCALADM," +
                "base.CREDLEVEL,base.ASSDate,base.ESDate,base.APPRdate,base.REGORG," +
                "base.ENTCAT,base.ENTSTATUS,base.REGCAP,base.OPSCOPE,base.OPFORM," +
                "base.OPSCOANDFORM,base.PTBUSSCOPE,base.DOMDISTRICT,base.DOM,base.ECOTECDEVZONE," +
                "base.DOMPRORIGHT,base.OPLOCDISTRICT,base.OPLOC,base.RECCAP,base.INSFORM," +
                "base.PARNUM,base.PARFORM,base.EXENUM,base.EMPNUM,base.SCONFORM," +
                "base.FORCAPINDCODE,base.MIDPREINDCODE,base.PROTYPE,base.CONGRO,base.CONGROCUR," +
                "base.CONGROUSD,base.REGCAPUSD,base.REGCAPCUR,base.REGCAPRMB,base.FORREGCAPCUR," +
                "base.FORREGCAPUSD,base.FORRECCAPUSD,base.WORCAP,base.CHAMECdate,base.OPRACTTYPE," +
                "base.FORENTNAME,base.DEPINCHA,base.COUNTRY,base.ITEMOFOPORCPRO,base.CONOFCONTRPRO," +
                "base.FORDOM,base.FORREGECAP,base.FOROPSCOPE,base.S_EXT_ENTPROPERTY,base.S_EXT_TIMESTAMP," +
                "base.S_EXT_BATCH,base.S_EXT_SEQUENCE,base.S_EXT_VALIDFLAG,base.S_EXT_INDUSCAT,base.S_EXT_ENTTYPE," +
                "base.MANACATE,base.LIMPARNUM,base.FOREIGNBODYTYPE,base.PERSON_ID,base.NAME," +
                "base.CERTYPE,base.CERNO,base.ANCHEYEAR,base.CANdate,base.REVdate," +
                "base.ENTNAME_OLD,base.CREDIT_CODE,base.JOBID,base.IS_NEW,base.COUNTRYDISPLAY," +
                "base.STATUSDISPLAY,base.TYPEDISPLAY,base.REGORGDISPLAY,base.REGCAPCURDISPLAY,base.ENTID," +
                "base.HANDLE_TYPE,base.TAX_CODE,base.LICID" +
                //"base.ZSPID" +
                " from NEW_ENT_BASEINFO_MID base " );
                /*" where esdate between '"+currYearDate+"' and '"+lastYearDate+"' " +
                " and  name is not null   and pripid is not  null  and entstatus = 1 " +
                " and ( regno is not null or credit_code is not null ) " );*/

        //2、将 df 转换为 rdd
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO_20170612
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {

                Connection conn = null;
                PreparedStatement st = null;

                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
                    conn.setAutoCommit(false);
                    String sql = "insert into NEW_ENT_BASEINFO_"+currYearDate+" ("+
                            "PRIPID," +
                            "S_EXT_NODENUM," +
                            "ENTNAME," +
                            "ORIREGNO," +
                            "REGNO," +
                            "ENTTYPE," +
                            "PPRIPID," +
                            "PENTNAME," +
                            "PREGNO," +
                            "HYPOTAXIS," +
                            "INDUSTRYPHY," +
                            "INDUSTRYCO," +
                            "ABUITEM," +
                            "CBUITEM," +
                            "OPFROM," +
                            "OPTO," +
                            "POSTALCODE," +
                            "TEL," +
                            "EMAIL," +
                            "LOCALADM," +
                            "CREDLEVEL," +
                            "ASSDATE," +
                            "ESDATE," +
                            "APPRDATE," +
                            "REGORG," +
                            "ENTCAT," +
                            "ENTSTATUS," +
                            "REGCAP," +
                            "OPSCOPE," +
                            "OPFORM," +
                            "OPSCOANDFORM," +
                            "PTBUSSCOPE," +
                            "DOMDISTRICT," +
                            "DOM," +
                            "ECOTECDEVZONE," +
                            "DOMPRORIGHT," +
                            "OPLOCDISTRICT," +
                            "OPLOC," +
                            "RECCAP," +
                            "INSFORM," +
                            "PARNUM," +
                            "PARFORM," +
                            "EXENUM," +
                            "EMPNUM," +
                            "SCONFORM," +
                            "FORCAPINDCODE," +
                            "MIDPREINDCODE," +
                            "PROTYPE," +
                            "CONGRO," +
                            "CONGROCUR," +
                            "CONGROUSD," +
                            "REGCAPUSD," +
                            "REGCAPCUR," +
                            "REGCAPRMB," +
                            "FORREGCAPCUR," +
                            "FORREGCAPUSD," +
                            "FORRECCAPUSD," +
                            "WORCAP," +
                            "CHAMECDATE," +
                            "OPRACTTYPE," +
                            "FORENTNAME," +
                            "DEPINCHA," +
                            "COUNTRY," +
                            "ITEMOFOPORCPRO," +
                            "CONOFCONTRPRO," +
                            "FORDOM," +
                            "FORREGECAP," +
                            "FOROPSCOPE," +
                            "S_EXT_ENTPROPERTY," +
                            "S_EXT_TIMESTAMP," +
                            "S_EXT_BATCH," +
                            "S_EXT_SEQUENCE," +
                            "S_EXT_VALIDFLAG," +
                            "S_EXT_INDUSCAT," +
                            "S_EXT_ENTTYPE," +
                            "MANACATE," +
                            "LIMPARNUM," +
                            "FOREIGNBODYTYPE," +
                            "PERSON_ID," +
                            "NAME," +
                            "CERTYPE," +
                            "CERNO," +
                            "ANCHEYEAR," +
                            "CANDATE," +
                            "REVDATE," +
                            "ENTNAME_OLD," +
                            "CREDIT_CODE," +
                            "JOBID," +
                            "IS_NEW," +
                            "COUNTRYDISPLAY," +
                            "STATUSDISPLAY," +
                            "TYPEDISPLAY," +
                            "REGORGDISPLAY," +
                            "REGCAPCURDISPLAY," +
                            "ENTID," +
                            "HANDLE_TYPE," +
                            "TAX_CODE," +
                            "LICID,BATCHDATE)" +
                            //"ZSPID) "+
                            "values("+
                            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                            "?,?,?,?,?,?,?,?,?)";//,?

                    st = conn.prepareStatement(sql);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        st.setString(1,row.getString(0));
                        st.setString(2,row.getString(1));
                        st.setString(3,row.getString(2));
                        st.setString(4,row.getString(3));
                        st.setString(5,row.getString(4));
                        st.setString(6,row.getString(5));
                        st.setString(7,row.getString(6));
                        st.setString(8,row.getString(7));
                        st.setString(9,row.getString(8));
                        st.setString(10,row.getString(9));
                        st.setString(11,row.getString(10));
                        st.setString(12,row.getString(11));
                        st.setString(13,row.getString(12));
                        st.setString(14,row.getString(13));
                        st.setDate(15,GeneralUtils.getColumnsDate(row.getString(14)));
                        st.setDate(16,GeneralUtils.getColumnsDate(row.getString(15)));

                        st.setString(17,row.getString(16));
                        st.setString(18,row.getString(17));
                        st.setString(19,row.getString(18));
                        st.setString(20,row.getString(19));
                        st.setString(21,row.getString(20));

                        st.setDate(22,GeneralUtils.getColumnsDate(row.getString(21)));
                        st.setDate(23,GeneralUtils.getColumnsDate(row.getString(22)));
                        st.setDate(24,GeneralUtils.getColumnsDate(row.getString(23)));


                        st.setString(25,row.getString(24));
                        st.setString(26,row.getString(25));
                        st.setString(27,row.getString(26));
                        st.setDouble(28,row.getDouble(27));
                        st.setString(29,row.getString(28));
                        st.setString(30,row.getString(29));
                        st.setString(31,row.getString(30));
                        st.setString(32,row.getString(31));
                        st.setString(33,row.getString(32));
                        st.setString(34,row.getString(33));
                        st.setString(35,row.getString(34));
                        st.setString(36,row.getString(35));
                        st.setString(37,row.getString(36));
                        st.setString(38,row.getString(37));
                        st.setDouble(39,row.getDouble(38));
                        st.setString(40,row.getString(39));
                        st.setInt(41,row.getInt(40));
                        st.setString(42,row.getString(41));
                        st.setInt(43,row.getInt(42));
                        st.setInt(44,row.getInt(43));
                        st.setString(45,row.getString(44));
                        st.setString(46,row.getString(45));
                        st.setString(47,row.getString(46));
                        st.setString(48,row.getString(47));
                        st.setDouble(49,row.getDouble(48));
                        st.setString(50,row.getString(49));
                        st.setDouble(51,row.getDouble(50));
                        st.setDouble(52,row.getDouble(51));
                        st.setString(53,row.getString(52));
                        st.setDouble(54,row.getDouble(53));
                        st.setString(55,row.getString(54));
                        st.setDouble(56,row.getDouble(55));
                        st.setDouble(57,row.getDouble(56));
                        st.setDouble(58,row.getDouble(57));

                        st.setDate(59,GeneralUtils.getColumnsDate(row.getString(58)));

                        st.setString(60,row.getString(59));
                        st.setString(61,row.getString(60));
                        st.setString(62,row.getString(61));
                        st.setString(63,row.getString(62));
                        st.setString(64,row.getString(63));
                        st.setString(65,row.getString(64));
                        st.setString(66,row.getString(65));
                        st.setDouble(67,row.getDouble(66));
                        st.setString(68,row.getString(67));
                        st.setString(69,row.getString(68));

                        st.setTimestamp(70, GeneralUtils.getColumnsTimeStamp(row.getString(69)));

                        st.setString(71,row.getString(70));
                        st.setString(72,row.getString(71));
                        st.setString(73,row.getString(72));
                        st.setString(74,row.getString(73));
                        st.setString(75,row.getString(74));
                        st.setString(76,row.getString(75));
                        st.setInt(77,row.getInt(76));
                        st.setString(78,row.getString(77));
                        st.setString(79,row.getString(78));
                        st.setString(80,row.getString(79));
                        st.setString(81,row.getString(80));
                        st.setString(82,row.getString(81));
                        st.setString(83,row.getString(82));

                        st.setDate(84,GeneralUtils.getColumnsDate(row.getString(83)));
                        st.setDate(85,GeneralUtils.getColumnsDate(row.getString(84)));


                        st.setString(86,row.getString(85));
                        st.setString(87,row.getString(86));
                        st.setString(88,row.getString(87));
                        st.setInt(89,row.getInt(88));
                        st.setString(90,row.getString(89));
                        st.setString(91,row.getString(90));
                        st.setString(92,row.getString(91));
                        st.setString(93,row.getString(92));
                        st.setString(94,row.getString(93));
                        st.setString(95,row.getString(94));
                        st.setInt(96,row.getInt(95));
                        st.setString(97,row.getString(96));
                        st.setString(98,row.getString(97));
                        st.setString(99,currYearDate);
                        //st.setString(99,row.getString(98));
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
        sc.stop();
    }

    private static String createTableStr(String currYearDate) {
         return  "create table NEW_ENT_BASEINFO_"+currYearDate+
                 " (" +
                 "PRIPID VARCHAR2(50)," +
                 "S_EXT_NODENUM VARCHAR2(40) not null," +
                 "ENTNAME VARCHAR2(2000)," +
                 "ORIREGNO VARCHAR2(200)," +
                 "REGNO VARCHAR2(50)," +
                 "ENTTYPE VARCHAR2(200)," +
                 "PPRIPID VARCHAR2(36)," +
                 "PENTNAME VARCHAR2(100)," +
                 "PREGNO VARCHAR2(200)," +
                 "HYPOTAXIS VARCHAR2(64)," +
                 "INDUSTRYPHY VARCHAR2(64)," +
                 "INDUSTRYCO VARCHAR2(64)," +
                 "ABUITEM VARCHAR2(4000)," +
                 "CBUITEM VARCHAR2(4000)," +
                 "OPFROM DATE," +
                 "OPTO DATE," +
                 "POSTALCODE VARCHAR2(200)," +
                 "TEL VARCHAR2(200)," +
                 "EMAIL VARCHAR2(100)," +
                 "LOCALADM VARCHAR2(64)," +
                 "CREDLEVEL VARCHAR2(64)," +
                 "ASSDATE DATE," +
                 "ESDATE DATE," +
                 "APPRDATE DATE," +
                 "REGORG VARCHAR2(4000)," +
                 "ENTCAT VARCHAR2(64)," +
                 "ENTSTATUS VARCHAR2(64)," +
                 "REGCAP NUMBER(26,6)," +
                 "OPSCOPE VARCHAR2(4000)," +
                 "OPFORM VARCHAR2(1000)," +
                 "OPSCOANDFORM VARCHAR2(4000)," +
                 "PTBUSSCOPE VARCHAR2(2000)," +
                 "DOMDISTRICT VARCHAR2(64)," +
                 "DOM VARCHAR2(4000)," +
                 "ECOTECDEVZONE VARCHAR2(64)," +
                 "DOMPRORIGHT VARCHAR2(200)," +
                 "OPLOCDISTRICT VARCHAR2(64)," +
                 "OPLOC VARCHAR2(600)," +
                 "RECCAP NUMBER(26,6)," +
                 "INSFORM VARCHAR2(64)," +
                 "PARNUM NUMBER(10)," +
                 "PARFORM VARCHAR2(64)," +
                 "EXENUM NUMBER(20)," +
                 "EMPNUM NUMBER(10)," +
                 "SCONFORM VARCHAR2(200)," +
                 "FORCAPINDCODE VARCHAR2(64)," +
                 "MIDPREINDCODE VARCHAR2(64)," +
                 "PROTYPE VARCHAR2(64)," +
                 "CONGRO NUMBER(26,6)," +
                 "CONGROCUR VARCHAR2(64)," +
                 "CONGROUSD NUMBER(26,6)," +
                 "REGCAPUSD NUMBER(26,6)," +
                 "REGCAPCUR VARCHAR2(64)," +
                 "REGCAPRMB NUMBER(26,6)," +
                 "FORREGCAPCUR VARCHAR2(64)," +
                 "FORREGCAPUSD NUMBER(26,6)," +
                 "FORRECCAPUSD NUMBER(26,6)," +
                 "WORCAP NUMBER(26,6)," +
                 "CHAMECDATE DATE," +
                 "OPRACTTYPE VARCHAR2(64)," +
                 "FORENTNAME VARCHAR2(200)," +
                 "DEPINCHA VARCHAR2(4000)," +
                 "COUNTRY VARCHAR2(128)," +
                 "ITEMOFOPORCPRO VARCHAR2(512)," +
                 "CONOFCONTRPRO VARCHAR2(3000)," +
                 "FORDOM VARCHAR2(200)," +
                 "FORREGECAP NUMBER(26,6)," +
                 "FOROPSCOPE VARCHAR2(1500)," +
                 "S_EXT_ENTPROPERTY VARCHAR2(64)," +
                 "S_EXT_TIMESTAMP TIMESTAMP(6) not null," +
                 "S_EXT_BATCH VARCHAR2(500)," +
                 "S_EXT_SEQUENCE VARCHAR2(40) not null," +
                 "S_EXT_VALIDFLAG VARCHAR2(1) not null," +
                 "S_EXT_INDUSCAT VARCHAR2(64)," +
                 "S_EXT_ENTTYPE VARCHAR2(64)," +
                 "MANACATE VARCHAR2(20)," +
                 "LIMPARNUM NUMBER(10)," +
                 "FOREIGNBODYTYPE VARCHAR2(200)," +
                 "PERSON_ID VARCHAR2(60)," +
                 "NAME VARCHAR2(2000)," +
                 "CERTYPE VARCHAR2(20)," +
                 "CERNO VARCHAR2(50)," +
                 "ANCHEYEAR VARCHAR2(10)," +
                 "CANDATE DATE," +
                 "REVDATE DATE," +
                 "ENTNAME_OLD VARCHAR2(1000)," +
                 "CREDIT_CODE VARCHAR2(50)," +
                 "JOBID VARCHAR2(200)," +
                 "IS_NEW NUMBER," +
                 "COUNTRYDISPLAY VARCHAR2(1000)," +
                 "STATUSDISPLAY VARCHAR2(1000)," +
                 "TYPEDISPLAY VARCHAR2(1000)," +
                 "REGORGDISPLAY VARCHAR2(1000)," +
                 "REGCAPCURDISPLAY VARCHAR2(1000)," +
                 "ENTID VARCHAR2(200)," +
                 "HANDLE_TYPE NUMBER(2)," +
                 "TAX_CODE VARCHAR2(30)," +
                 "LICID VARCHAR2(30)," +
                 "ZSPID VARCHAR2(4000) default NULL，" +
                 "BATCHDATE VARCHAR2(10)" +
                 " )";
    }
}
