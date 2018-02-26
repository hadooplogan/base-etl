package com.chinadaas.common.register;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.table.*;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;


/**
 * Created by gongxs01 on 2017/9/22.
 */
public class RegisterTable implements Serializable {

    private static final String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);

    public static String ENT_INFO = "entInfoTmp03";
    public static String ENT_INV_INFO = "entInvTmp";
    public static String ENT_PERSON_INFO = "entPersonTmp";

    public static String ABNORMITY = "abnormity";
    public static String BREAKLAW = "breaklaw";
    public static String BZXR = "bzxr";
    public static String SXBZR = "sxbzr";
    public static final String regext = "\\u0001";


    public static void loadAndRegiserTable(SparkSession sqlContext, String[] tableNames) {
        for (int i = 0; i < tableNames.length; i++) {
            sqlContext.read().parquet(parquetPath + tableNames[i]).registerTempTable(tableNames[i]);
        }
    }

    //人员管理表

    /**
     * @param spark
     * @param tableNames 临时表名
     * @param date       批次日期
     */
    public static Long regiserEpriPersonTable(SparkSession spark, String tableNames, final String date, String path) {
        final LongAccumulator entaccumulator = spark.sparkContext().longAccumulator();

        JavaRDD<E_PRI_PERSON> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_PRI_PERSON>() {
            @Override
            public E_PRI_PERSON call(String row) throws Exception {
                entaccumulator.add(1);
                return converEpriPseron(row, date);
            }
        });
        spark.createDataFrame(persons, E_PRI_PERSON.class).registerTempTable(tableNames + "");

      /*  if("all".equals(flag)){
            RegisterTableETL.getRegiserFullTable(spark,tableNames,tableNames+"_tmp",date);
        }else{
            RegisterTableETL.getRegiserTable(spark,tableNames);
        }*/

        DataFrameUtil.getDataFrame(spark, "select * from e_pri_person", "e_pri_person", DataFrameUtil.CACHETABLE_PARQUET);

        return entaccumulator.count();
    }


    public static E_PRI_PERSON converEpriPseron(String row, String date) {
        String[] person = row.split(regext, -1);
        return new E_PRI_PERSON(person[0], person[1], person[2], person[3], person[4]
                , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                person[13], person[14], person[15], person[16], person[17], person[18],
                person[19], person[20], person[21], person[22], person[23], person[24],
                person[25], person[26], person[27], person[28], person[29], person[30],
                person[31], person[32], person[33], person[34], person[35], person[36],
                person[37], person[38], date);
    }


    //企业基本信息表
    public static Long regiserEntBaseInfoTable(SparkSession spark, String tableNames, final String date, String path) {

        final LongAccumulator entaccumulator = spark.sparkContext().longAccumulator();

        JavaRDD<ENTERPRISEBASEINFOCOLLECT> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, ENTERPRISEBASEINFOCOLLECT>() {
            @Override
            public ENTERPRISEBASEINFOCOLLECT call(String row) throws Exception {

                entaccumulator.add(1);
                return converEntBaseInfo(row, date);
            }
        });
        spark.createDataFrame(persons, ENTERPRISEBASEINFOCOLLECT.class).registerTempTable(tableNames);

        /*if("all".equals(flag)){
            RegisterTableETL.getRegiserFullTable(spark,tableNames,tableNames+"_tmp",date);
        }else{
            //因为是分区表会有重复数据需要按最新日期去重操作
            RegisterTableETL.getRegiserTable(spark,tableNames);
        }*/
        DataFrameUtil.getDataFrame(spark, "select * from enterprisebaseinfocollect", "enterprisebaseinfocollect", DataFrameUtil.CACHETABLE_PARQUET);
        return entaccumulator.count();
    }

    public static ENTERPRISEBASEINFOCOLLECT converEntBaseInfo(String row, String date) {


        String[] person = row.split(regext, -1);

        return new ENTERPRISEBASEINFOCOLLECT(person[0], person[1], person[2], person[3], person[4]
                , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                person[13], person[14], person[15], person[16], person[17], person[18],
                person[19], person[20], person[21], person[22], person[23], person[24],
                person[25], person[26], person[27], person[28], person[29], person[30],
                person[31], person[32], person[33], person[34], person[35], person[36],
                person[37], person[38], person[39], person[40], person[41], person[42],
                person[43], person[44], person[45], person[46], person[47], person[48],
                person[49], person[50], person[51], person[52], person[53], person[54],
                person[55], person[56], person[57], person[58], person[59], person[60],
                person[61], person[62], person[63], person[64], person[65], person[66],
                person[67], person[68], person[69], person[70], person[71], person[72],
                person[73], person[74], person[75], person[76], person[77], person[78],
                person[79], person[80], person[81], person[82], person[83], person[84],
                person[85], person[86], person[87], person[88], person[89], person[90],
                person[91], person[92], person[93], date);
    }

    //股东基本信息表

    public static Long regiserInvTable(SparkSession spark, String tableNames, final String date, String path) {
        final LongAccumulator entaccumulator = spark.sparkContext().longAccumulator();
        JavaRDD<E_INV_INVESTMENT> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_INV_INVESTMENT>() {
            @Override
            public E_INV_INVESTMENT call(String row) throws Exception {
                entaccumulator.add(1);

                return converEntInv(row, date);

            }
        });
        spark.createDataFrame(persons, E_INV_INVESTMENT.class).registerTempTable(tableNames);
       /* if("all".equals(flag)){
            RegisterTableETL.getRegiserFullTable(spark,tableNames,tableNames+"_tmp",date);
        }else{
            //因为是分区表会有重复数据需要按最新日期去重操作
            RegisterTableETL.getRegiserTable(spark,tableNames);
        }*/
        DataFrameUtil.getDataFrame(spark, "select * from e_inv_investment", "e_inv_investment", DataFrameUtil.CACHETABLE_PARQUET);
        return entaccumulator.count();
    }


    public static E_INV_INVESTMENT converEntInv(String row, String date) {

        String[] person = row.split(regext, -1);

        return new E_INV_INVESTMENT(person[0], person[1], person[2], person[3], person[4]
                , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                person[13], person[14], person[15], person[16], person[17], person[18],
                person[19], person[20], person[21], person[22], person[23], person[24],
                person[25], person[26], person[27], person[28], person[29], person[30],
                person[31], person[32], person[33], date);
    }


    //企业变更信息表
    public static void regiserAlterRecoderTable(SparkSession spark, String tableNames, final String date, String path) {
        JavaRDD<E_ALTER_RECODER> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_ALTER_RECODER>() {
            @Override
            public E_ALTER_RECODER call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new E_ALTER_RECODER(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7],
                        person[8], person[9], person[10], person[11], date);

            }
        });

        spark.createDataFrame(persons, E_ALTER_RECODER.class).registerTempTable(tableNames + "");
        DataFrameUtil.getDataFrame(spark, "select * from e_alter_recoder", "e_alter_recoder", DataFrameUtil.CACHETABLE_PARQUET);
    }

    //有14个字段
    public static void registerCaseBaseInfo(SparkSession spark, String tableNames, String path) {
        JavaRDD<E_CASEBASEINFO> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_CASEBASEINFO>() {
            @Override
            public E_CASEBASEINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);
                return new E_CASEBASEINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13]);

            }
        });

        spark.createDataFrame(persons, E_CASEBASEINFO.class).registerTempTable(tableNames + "");
    }

    //企业行政处罚案件基本信息表 ()
    public static void registerCaseParty(SparkSession spark, String tableNames, String path) {
        JavaRDD<E_CASEPARTYINFO> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_CASEPARTYINFO>() {
            @Override
            public E_CASEPARTYINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);

                return new E_CASEPARTYINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8]);
            }
        });

        spark.createDataFrame(persons, E_CASEPARTYINFO.class).registerTempTable(tableNames + "");
    }

    //企业商标信息表（表里有37个字段）
    public static void registerTradeMarkInfo(SparkSession spark, String tableNames, String path) {


     /**   JavaRDD<E_TRADEMARKINGO> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 37) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, E_TRADEMARKINGO>() {
            @Override
            public E_TRADEMARKINGO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);

                return new E_TRADEMARKINGO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36]);
            }
        }); */
        JavaRDD<E_TRADEMARKINGO> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_TRADEMARKINGO>() {
            @Override
            public E_TRADEMARKINGO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);

                return new E_TRADEMARKINGO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36]);
            }
        });
        spark.createDataFrame(persons, E_TRADEMARKINGO.class).registerTempTable(tableNames + "");
    }

    //作品著作权登记人信息表（表里面有23个字段）
    public static void registerProductCopyRightInfo(SparkSession spark, String tableNames, String path) {
        JavaRDD<E_PRODUCTCOPYRIGHTINFO> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 23) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, E_PRODUCTCOPYRIGHTINFO>() {

            @Override
            public E_PRODUCTCOPYRIGHTINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);
                return new E_PRODUCTCOPYRIGHTINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22]);
            }
        });

        spark.createDataFrame(persons, E_PRODUCTCOPYRIGHTINFO.class).registerTempTable(tableNames + "");
    }

    public static Long regiserGtEntBaseInfoTable(SparkSession spark, String tableNames, final String date, String path) {
        final LongAccumulator entaccumulator = spark.sparkContext().longAccumulator();
        JavaRDD<E_GT_BASEINFO> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_GT_BASEINFO>() {
            @Override
            public E_GT_BASEINFO call(String row) throws Exception {
                String[] person = row.toString().split(regext, -1);
                entaccumulator.add(1);
                return new E_GT_BASEINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36],
                        person[37], person[38], person[39], person[40], person[41], person[42],
                        person[43], person[44], person[45], person[46], date);
            }
        });
        spark.createDataFrame(persons, E_GT_BASEINFO.class).registerTempTable(tableNames + "");

        DataFrameUtil.getDataFrame(spark, "select * from e_gt_baseinfo", "e_gt_baseinfo", DataFrameUtil.CACHETABLE_PARQUET);

        return entaccumulator.count();
    }

    //个体人员管理表
    public static Long regiserGtPersonTable(SparkSession spark, String tableNames, final String date, String path) {
        final LongAccumulator entaccumulator = spark.sparkContext().longAccumulator();
        JavaRDD<E_GT_PERSON> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_GT_PERSON>() {
            @Override
            public E_GT_PERSON call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                entaccumulator.add(1);
                return new E_GT_PERSON(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], date);
            }
        });
        spark.createDataFrame(persons, E_GT_PERSON.class).registerTempTable(tableNames + "");
        DataFrameUtil.getDataFrame(spark, "select * from e_gt_person", "e_gt_person", DataFrameUtil.CACHETABLE_PARQUET);
        return entaccumulator.count();
    }


    //软件著作权信息登记表(表里有17个字段)
    public static void registerCopyRightInfo(SparkSession spark, String tableNames, String path) {
        JavaRDD<E_COPYRIGHTINFO> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 17) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, E_COPYRIGHTINFO>() {
            @Override
            public E_COPYRIGHTINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);
                return new E_COPYRIGHTINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16]);
            }
        });

        spark.createDataFrame(persons, E_COPYRIGHTINFO.class).registerTempTable(tableNames + "");

    }


    //软件著作权登记人信息表(表里有15个字段)
    public static void registerCopyRightOrg(SparkSession spark, String tableName, String path) {
        JavaRDD<E_COPYRIGHTORG> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 15) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, E_COPYRIGHTORG>() {
            @Override
            public E_COPYRIGHTORG call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);
                return new E_COPYRIGHTORG(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14]);
            }
        });

        spark.createDataFrame(persons, E_COPYRIGHTORG.class).registerTempTable(tableName + "");

    }

    //v1_code 码值转化表
    public static void regiserInMapTable(SparkSession spark, String tableNames, String path) {
        JavaRDD<S_CIF_INDMAP> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_CIF_INDMAP>() {
            @Override
            public S_CIF_INDMAP call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_CIF_INDMAP(person[0], person[1], person[2], person[3]);

            }
        });

        spark.createDataFrame(persons, S_CIF_INDMAP.class).registerTempTable(tableNames + "");

        spark.sql("select * from S_CIF_INDMAP_T").limit(10).show();

        DataFrameUtil.getDataFrame(spark, "select encode_v1,zspid from S_CIF_INDMAP_T", "S_CIF_INDMAP", DataFrameUtil.CACHETABLE_PARQUET);
    }


    /**
     * 严重违法
     *
     * @param spark
     * @param tableNames
     * @param path
     */
    public static void regiserBreakLawTable(SparkSession spark, String tableNames, String path) {
        JavaRDD<S_EN_BREAK_LAW> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_EN_BREAK_LAW>() {
            @Override
            public S_EN_BREAK_LAW call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_EN_BREAK_LAW(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7], person[8], person[9],
                        person[10], person[11], person[12], person[13], person[14], person[15],
                        person[16], person[17], person[18], person[19], person[20], person[21], person[22]);
            }
        });
        spark.createDataFrame(persons, S_EN_BREAK_LAW.class).registerTempTable(tableNames + "");

    }


    /**
     * 经营异常名录
     *
     * @param spark
     * @param tableNames
     * @param path
     */
    public static void regiserAbnormityTable(SparkSession spark, String tableNames, String path) {
        JavaRDD<S_EN_ABNORMITY> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_EN_ABNORMITY>() {
            @Override
            public S_EN_ABNORMITY call(String row) throws Exception {
                String[] person = row.split(regext, -1);

                return new S_EN_ABNORMITY(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7], person[8], person[9],
                        person[10], person[11], person[12], person[13], person[14], person[15],
                        person[16], person[17], person[18], person[19], person[20], person[21], person[22]);
            }
        });
        spark.createDataFrame(persons, S_EN_ABNORMITY.class).registerTempTable(tableNames + "");

    }

    /**
     * 被执行人
     *
     * @param spark
     * @param tableNames
     * @param path
     */
    public static void regiserBZXRTable(SparkSession spark, String tableNames, String path) {
        JavaRDD<DIS_BZXR_NEW> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 23) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, DIS_BZXR_NEW>() {
            @Override
            public DIS_BZXR_NEW call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new DIS_BZXR_NEW(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7], person[8], person[9],
                        person[10], person[11], person[12], person[13], person[14], person[15],
                        person[16], person[17], person[18], person[19], person[20], person[21]);
            }
        });
        spark.createDataFrame(persons, DIS_BZXR_NEW.class).registerTempTable(tableNames + "");
    }

    /**
     * 失信被执行人
     *
     * @param spark
     * @param tableNames
     * @param path
     */
    public static void regiserSXBZXRTable(SparkSession spark, String tableNames, String path) {
        JavaRDD<DIS_SXBZXR_NEW> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 31) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, DIS_SXBZXR_NEW>() {
            @Override
            public DIS_SXBZXR_NEW call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new DIS_SXBZXR_NEW(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7], person[8], person[9],
                        person[10], person[11], person[12], person[13], person[14], person[15],
                        person[16], person[17], person[18], person[19], person[20], person[21], person[22], person[23],
                        person[24], person[25], person[26], person[27], person[28], person[29]);
            }
        });
        spark.createDataFrame(persons, DIS_SXBZXR_NEW.class).registerTempTable(tableNames + "");
    }


    /**
     * 专利信息主表
     *
     * @param spark
     * @param tableNames
     * @param path       51个字段
     *                   2018-0105
     */

    public static void registerS_SIPO_PATENT_INFO(SparkSession spark, String tableNames, String path) {
        JavaRDD<S_SIPO_PATENT_INFO> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 51) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, S_SIPO_PATENT_INFO>() {
            @Override
            public S_SIPO_PATENT_INFO call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_SIPO_PATENT_INFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36],
                        person[37], person[38], person[39], person[40], person[41], person[42],
                        person[43], person[44], person[45], person[46], person[47], person[48],
                        person[49], person[50]);
            }
        });
        spark.createDataFrame(persons, S_SIPO_PATENT_INFO.class).registerTempTable(tableNames + "");

    }


    /**
     * 专利权人信息表
     *
     * @param spark
     * @param tableNames
     * @param path       17个字段
     *                   2018-0105
     */
    //专利权人信息表 17个字段 2018-01-08
    public static void registerS_SIPO_PATENT_COPYRIGHT(SparkSession spark, String tableNames, String path) {

        JavaRDD<S_SIPO_PATENT_COPYRIGHT> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 17) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, S_SIPO_PATENT_COPYRIGHT>() {
            @Override
            public S_SIPO_PATENT_COPYRIGHT call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_SIPO_PATENT_COPYRIGHT(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16]);
            }
        });

        spark.createDataFrame(persons, S_SIPO_PATENT_COPYRIGHT.class).registerTempTable(tableNames + "");

    }

    /**
     * 专利法律信息表
     *
     * @param spark
     * @param tableNames
     * @param path       51个字段
     *                   2018-0105
     */
    //专利法律信息表 13个字段 2018-01-08
    public static void registerS_SIPO_PATENT_LAWSTATE(SparkSession spark, String tableNames, String path) {
        JavaRDD<S_SIPO_PATENT_LAWSTATE> persons = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.split(regext, -1).length == 13) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, S_SIPO_PATENT_LAWSTATE>() {
            @Override
            public S_SIPO_PATENT_LAWSTATE call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_SIPO_PATENT_LAWSTATE(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12]);
            }
        });


        spark.createDataFrame(persons, S_SIPO_PATENT_LAWSTATE.class).registerTempTable(tableNames + "");


    }

    //企业主表 @xinghao
    public static void registerBaseInfoTable(SparkSession spark, String tableNames, final String date, String path) {
        JavaRDD<ENTERPRISEBASEINFOCOLLECT> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, ENTERPRISEBASEINFOCOLLECT>() {
            @Override
            public ENTERPRISEBASEINFOCOLLECT call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new ENTERPRISEBASEINFOCOLLECT(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36],
                        person[37], person[38], person[39], person[40], person[41], person[42],
                        person[43], person[44], person[45], person[46], person[47], person[48],
                        person[49], person[50], person[51], person[52], person[53], person[54],
                        person[55], person[56], person[57], person[58], person[59], person[60],
                        person[61], person[62], person[63], person[64], person[65], person[66],
                        person[67], person[68], person[69], person[70], person[71], person[72],
                        person[73], person[74], person[75], person[76], person[77], person[78],
                        person[79], person[80], person[81], person[82], person[83], person[84],
                        person[85], person[86], person[87], person[88], person[89], person[90],
                        person[91], person[92], person[93], date);
            }
        });
        spark.createDataFrame(persons, ENTERPRISEBASEINFOCOLLECT.class).registerTempTable(tableNames + "");

    }

    //股东表 @xinghao
    public static void registerInvTable(SparkSession spark, String tableNames, final String date, String path) {
        JavaRDD<E_INV_INVESTMENT> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_INV_INVESTMENT>() {
            @Override
            public E_INV_INVESTMENT call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new E_INV_INVESTMENT(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], date);
            }
        });

        spark.createDataFrame(persons, E_INV_INVESTMENT.class).registerTempTable(tableNames + "");
    }


    public static void registerEpriPersonTable(SparkSession spark, String tableNames, final String date, String path) {
        JavaRDD<E_PRI_PERSON> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_PRI_PERSON>() {
            @Override
            public E_PRI_PERSON call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new E_PRI_PERSON(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36],
                        person[37], person[38], date);
            }
        });

        spark.createDataFrame(persons, E_PRI_PERSON.class).registerTempTable(tableNames + "");

    }

    //企业变更表 @xinghao
    public static void registerAlterRecoderTable(SparkSession spark, String tableNames, final String date, String path) {
        JavaRDD<E_ALTER_RECODER> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_ALTER_RECODER>() {
            @Override
            public E_ALTER_RECODER call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new E_ALTER_RECODER(person[0], person[1], person[2], person[3],
                        person[4], person[5], person[6], person[7],
                        person[8], person[9], person[10], person[11], date);

            }
        });
        spark.createDataFrame(persons, E_ALTER_RECODER.class).registerTempTable(tableNames + "");

    }

    //企业年报表
    public static void registerAnnreportTable(SparkSession spark,String tableNames,String path){

        JavaRDD<E_ANNREPORT_BASEINFO> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, E_ANNREPORT_BASEINFO>() {
            @Override
            public E_ANNREPORT_BASEINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);
                return new E_ANNREPORT_BASEINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16]);
            }
        });

        spark.createDataFrame(persons,E_ANNREPORT_BASEINFO.class).registerTempTable(tableNames+"");


    }

    //税务登记表
    public static void registerTaxBaseinfo(SparkSession spark,String tableNames,String path){

        JavaRDD<S_TAX_BASEINFO> person = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_TAX_BASEINFO>() {

            @Override
            public S_TAX_BASEINFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);

                return new S_TAX_BASEINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8]);
            }
        });

 spark.createDataFrame(person,S_TAX_BASEINFO.class).registerTempTable(tableNames+"");
    }


    //组织机构
    public static void registerS_OGZ_BASEINFO(SparkSession spark,String tablenames,String path){

        JavaRDD<S_OGZ_BASEINFO> person = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_OGZ_BASEINFO>() {
            @Override
            public S_OGZ_BASEINFO call(String v1) throws Exception {
                String[] person = v1.split("\\t", -1);
                return new S_OGZ_BASEINFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30]);
            }
        });

        spark.createDataFrame(person,S_OGZ_BASEINFO.class).registerTempTable(tablenames+"");

    }



//曾用名
 public static void registerS_EN_USEDNAME(SparkSession spark,String tableNames,String path){

     JavaRDD<S_EN_USEDNAME> person = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
         @Override
         public Boolean call(String s) throws Exception {
             if (s.split(regext, -1).length == 4) {
                 return true;
             } else {
                 return false;
             }
         }
     }).map(new Function<String, S_EN_USEDNAME>() {
         @Override
         public S_EN_USEDNAME call(String row) throws Exception {

             String[] person = row.split(regext, -1);
             return new S_EN_USEDNAME(person[0], person[1], person[2], person[3]);

         }
     });

     spark.createDataFrame(person,S_EN_USEDNAME.class).registerTempTable(tableNames+"");
 }

 //上市数据《机构资料表TQ_COMP_INFO》

    public static void registerTQ_COMP_INFO(SparkSession spark,String tableNames,String path){

        JavaRDD<TQ_COMP_INFO> person = spark.read().textFile(path).toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                if (v1.split(regext, -1).length == 60) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new Function<String, TQ_COMP_INFO>() {


            @Override
            public TQ_COMP_INFO call(String v1) throws Exception {
                String[] person = v1.split(regext, -1);

                return new TQ_COMP_INFO(person[0], person[1], person[2], person[3], person[4]
                        , person[5], person[6], person[7], person[8], person[9], person[10], person[11], person[12],
                        person[13], person[14], person[15], person[16], person[17], person[18],
                        person[19], person[20], person[21], person[22], person[23], person[24],
                        person[25], person[26], person[27], person[28], person[29], person[30],
                        person[31], person[32], person[33], person[34], person[35], person[36],
                        person[37], person[38], person[39], person[40], person[41], person[42],
                        person[43], person[44], person[45], person[46], person[47], person[48],
                        person[49], person[50], person[51], person[52], person[53], person[54],
                        person[55], person[56], person[57], person[58], person[59]);
            }
        });

      spark.createDataFrame(person,TQ_COMP_INFO.class).registerTempTable(tableNames+"");
    }

    /**
     *  曾用名表（S_EN_USEDNAME）
     *
     * @param spark
     * @param tableNames 注册表名
     * @param path   路径全量或者增量路径
     */
    public static void registerUsednameTable(SparkSession spark, String tableNames, String path){
        JavaRDD<S_EN_USEDNAME> persons = spark.read().textFile(path).toJavaRDD().map(new Function<String, S_EN_USEDNAME>() {
            @Override
            public S_EN_USEDNAME call(String row) throws Exception {
                String[] person = row.split(regext, -1);
                return new S_EN_USEDNAME(person[0], person[1], person[2], person[3]);

            }
        });
        spark.createDataFrame(persons, S_EN_USEDNAME.class).registerTempTable(tableNames + "");

    }

}