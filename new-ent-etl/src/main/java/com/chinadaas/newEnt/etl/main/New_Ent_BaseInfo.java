package com.chinadaas.newEnt.etl.main;

import com.chinadaas.newEnt.etl.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;
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

        //1、加载 hive表中数据到dataFrame
        DataFrame df = hc.sql("select  \n" +
                "base.PRIPID,\n" +
                "base.S_EXT_NODENUM,\n" +
                "base.ENTNAME,\n" +
                "base.ORIREGNO,\n" +
                "base.REGNO,\n" +
                "base.ENTTYPE,\n" +
                "base.PPRIPID,\n" +
                "base.PENTNAME,\n" +
                "base.PREGNO,\n" +
                "base.HYPOTAXIS,\n" +
                "base.INDUSTRYPHY,\n" +
                "base.INDUSTRYCO,\n" +
                "base.ABUITEM,\n" +
                "base.CBUITEM,\n" +
                "base.OPFROM,\n" +
                "base.OPTO,\n" +
                "base.POSTALCODE,\n" +
                "base.TEL,\n" +
                "base.EMAIL,\n" +
                "base.LOCALADM,\n" +
                "base.CREDLEVEL,\n" +
                "base.ASSDate,\n" +
                "base.ESDate,\n" +
                "base.APPRdate,\n" +
                "base.REGORG,\n" +
                "base.ENTCAT,\n" +
                "base.ENTSTATUS,\n" +
                "base.REGCAP,\n" +
                "base.OPSCOPE,\n" +
                "base.OPFORM,\n" +
                "base.OPSCOANDFORM,\n" +
                "base.PTBUSSCOPE,\n" +
                "base.DOMDISTRICT,\n" +
                "base.DOM,\n" +
                "base.ECOTECDEVZONE,\n" +
                "base.DOMPRORIGHT,\n" +
                "base.OPLOCDISTRICT,\n" +
                "base.OPLOC,\n" +
                "base.RECCAP,\n" +
                "base.INSFORM,\n" +
                "base.PARNUM,\n" +
                "base.PARFORM,\n" +
                "base.EXENUM,\n" +
                "base.EMPNUM,\n" +
                "base.SCONFORM,\n" +
                "base.FORCAPINDCODE,\n" +
                "base.MIDPREINDCODE,\n" +
                "base.PROTYPE,\n" +
                "base.CONGRO,\n" +
                "base.CONGROCUR,\n" +
                "base.CONGROUSD,\n" +
                "base.REGCAPUSD,\n" +
                "base.REGCAPCUR,\n" +
                "base.REGCAPRMB,\n" +
                "base.FORREGCAPCUR,\n" +
                "base.FORREGCAPUSD,\n" +
                "base.FORRECCAPUSD,\n" +
                "base.WORCAP,\n" +
                "base.CHAMECdate,\n" +
                "base.OPRACTTYPE,\n" +
                "base.FORENTNAME,\n" +
                "base.DEPINCHA,\n" +
                "base.COUNTRY,\n" +
                "base.ITEMOFOPORCPRO,\n" +
                "base.CONOFCONTRPRO,\n" +
                "base.FORDOM,\n" +
                "base.FORREGECAP,\n" +
                "base.FOROPSCOPE,\n" +
                "base.S_EXT_ENTPROPERTY,\n" +
                "base.S_EXT_TIMESTAMP,\n" +
                "base.S_EXT_BATCH,\n" +
                "base.S_EXT_SEQUENCE,\n" +
                "base.S_EXT_VALIDFLAG,\n" +
                "base.S_EXT_INDUSCAT,\n" +
                "base.S_EXT_ENTTYPE,\n" +
                "base.MANACATE,\n" +
                "base.LIMPARNUM,\n" +
                "base.FOREIGNBODYTYPE,\n" +
                "base.PERSON_ID,\n" +
                "base.NAME,\n" +
                "base.CERTYPE,\n" +
                "base.CERNO,\n" +
                "base.ANCHEYEAR,\n" +
                "base.CANdate,\n" +
                "base.REVdate,\n" +
                "base.ENTNAME_OLD,\n" +
                "base.CREDIT_CODE,\n" +
                "base.JOBID,\n" +
                "base.IS_NEW,\n" +
                "base.COUNTRYDISPLAY,\n" +
                "base.STATUSDISPLAY,\n" +
                "base.TYPEDISPLAY,\n" +
                "base.REGORGDISPLAY,\n" +
                "base.REGCAPCURDISPLAY,\n" +
                "base.ENTID,\n" +
                "base.HANDLE_TYPE,\n" +
                "base.TAX_CODE,\n" +
                "base.LICID" +
                //"base.ZSPID\n" +
                " from NEW_ENT_BASEINFO base " +
                " where esdate between '20160612' and '20170612' " +
                " and  name is not null   and pripid is not  null  and entstatus = 1 " +
                " and (regno is not null or credit_code is not null) " );

        //2、将 df 转换为 rdd
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO_20170612
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {

                JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                String sql = "insert into NEW_ENT_BASEINFO_20170619_TEST ("+
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
                        "LICID)" +
                        //"ZSPID) "+
                        "values("+
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                        "?,?,?,?,?,?,?,?)";//,?

                List<Object[]> params = new ArrayList<>();

                    while (rows.hasNext()) {
                        Row row = rows.next();

                        params.add(new Object[]{
                                row.getString(0),
                                row.getString(1),
                                row.getString(2),
                                row.getString(3),
                                row.getString(4),
                                row.getString(5),
                                row.getString(6),
                                row.getString(7),
                                row.getString(8),
                                row.getString(9),
                                row.getString(10),
                                row.getString(11),
                                row.getString(12),
                                row.getString(13),
                                GeneralUtils.getColumnsDate(row.getString(14)),
                                GeneralUtils.getColumnsDate(row.getString(15)),
                                row.getString(16),
                                row.getString(17),
                                row.getString(18),
                                row.getString(19),
                                row.getString(20),
                                GeneralUtils.getColumnsDate(row.getString(21)),
                                GeneralUtils.getColumnsDate(row.getString(22)),
                                GeneralUtils.getColumnsDate(row.getString(23)),
                                row.getString(24),
                                row.getString(25),
                                row.getString(26),
                                row.getDouble(27),
                                row.getString(28),
                                row.getString(29),
                                row.getString(30),
                                row.getString(31),
                                row.getString(32),
                                row.getString(33),
                                row.getString(34),
                                row.getString(35),
                                row.getString(36),
                                row.getString(37),
                                row.getDouble(38),
                                row.getString(39),
                                row.getInt(40),
                                row.getString(41),
                                row.getInt(42),
                                row.getInt(43),
                                row.getString(44),
                                row.getString(45),
                                row.getString(46),
                                row.getString(47),
                                row.getDouble(48),
                                row.getString(49),
                                row.getDouble(50),
                                row.getDouble(51),
                                row.getString(52),
                                row.getDouble(53),
                                row.getString(54),
                                row.getDouble(55),
                                row.getDouble(56),
                                row.getDouble(57),
                                GeneralUtils.getColumnsDate(row.getString(58)),
                                row.getString(59),
                                row.getString(60),
                                row.getString(61),
                                row.getString(62),
                                row.getString(63),
                                row.getString(64),
                                row.getString(65),
                                row.getDouble(66),
                                row.getString(67),
                                row.getString(68),
                                GeneralUtils.getColumnsTimeStamp(row.getString(69)),
                                row.getString(70),
                                row.getString(71),
                                row.getString(72),
                                row.getString(73),
                                row.getString(74),
                                row.getString(75),
                                row.getInt(76),
                                row.getString(77),
                                row.getString(78),
                                row.getString(79),
                                row.getString(80),
                                row.getString(81),
                                row.getString(82),
                                GeneralUtils.getColumnsDate(row.getString(83)),
                                GeneralUtils.getColumnsDate(row.getString(84)),
                                row.getString(85),
                                row.getString(86),
                                row.getString(87),
                                row.getInt(88),
                                row.getString(89),
                                row.getString(90),
                                row.getString(91),
                                row.getString(92),
                                row.getString(93),
                                row.getString(94),
                                row.getInt(95),
                                row.getString(96),
                                row.getString(97)
                        });
                    }
                    jdbcHelper.executeBatch(sql, params);
            }
        });
        sc.stop();
    }
}
