package com.chinadaas.next.etl.sparksql.convert.impl;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.next.etl.sparksql.sql.EntRelationBiKpiETL;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public class EntRelationCovertImpl extends AbstractConvert{

    final static String LEGAL = "legal";
    final static String PERSONINV = "personinv";

    final static String ENTINV = "entinv";

    final static String EDDR = "entaddr";
    final static String ETEL = "etel";
    final static String PADDR = "paddr";
    final static String PERSON = "person";
    final static String ENT = "ent";

    final static String defalt = "true";

    final static String MERGE = "merge";

    final static String ORG = "org";

    /**
     * 数据转换
     * @param spark
     */
    @Override
    public void convertData(SparkSession spark,String date) {
        registerTmpTable(spark,date);

//        adapterParquet.writeData(EntRelationBiKpiETL.getEntRelationBiKpi(spark),CommonConfig.getValue(DatabaseValues.ENT_INDEX_DIR));

    }


    //注册关联关系临时表，并将特殊字符转化
    //chinadaas.association.relation.personmerge.header=:START_ID(Person-ID)|riskscore|:END_ID(Ent-ID)
   // chinadaas.association.relation.invmerge.header=:START_ID(Ent-ID)|riskscore|:END_ID(Ent-ID)
    //chinadaas.association.relation.orgmerge.header=:START_ID(Org-ID)|riskscore|:END_ID(Ent-ID)
    //zsid:ID(Org-ID)|name|type|entstatus
    private void registerTmpTable(SparkSession spark,String date){

        String path = DST_PATH+date+"/";

//        this.adapter.loadData(spark,getCfgMap(path+EDDR)).
//                withColumnRenamed("value:ID(Ent-AddrID)","value").createOrReplaceTempView(EDDR);
//
//        this.adapter.loadData(spark,getCfgMap(path+ETEL)).
//                withColumnRenamed("value:ID(Ent-TelID)","value").createOrReplaceTempView(ETEL);
//
//        this.adapter.loadData(spark,getCfgMap(path+PADDR)).
//                withColumnRenamed("value:ID(Person-AddrID)","value").createOrReplaceTempView(PADDR);

        String srcPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_SRCPATH_TMP);
        String dstPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_DSTPATH_TMP)+date;



        //regno|creditcode|esdate|industryphy|regcap|entstatus|regcapcur|riskinfo|islist
        ;

//        this.adapter.loadData(spark,getCfgMap(path+ORG)).drop("name");



        DataFrameUtil.saveAsCsv(this.adapter.loadData(spark,getCfgMap(path+ENT)).drop("riskinfo").repartition(1),"/relation/dstpath/gxs/ent");



        DataFrameUtil.saveAsCsv(this.adapter.loadData(spark,getCfgMap(path+PERSON)).drop("riskinfo").repartition(1),"/relation/dstpath/gxs/person");


//        this.adapter.loadData(spark,getCfgMap(path+"personmerge")).
//                withColumnRenamed(":START_ID(Person-ID)","startkey").
//                withColumnRenamed(":END_ID(Ent-ID)","endKey").registerTempTable("personmerge");
//
//        this.adapter.loadData(spark,getCfgMap(path+"invmerge")).
//                withColumnRenamed(":START_ID(Ent-ID)","startkey").
//                withColumnRenamed(":END_ID(Ent-ID)","endKey").registerTempTable("invmerge");
//
//        this.adapter.loadData(spark,getCfgMap(path+"orgmerge")).
//                withColumnRenamed(":START_ID(Org-ID)","startkey").
//                withColumnRenamed(":END_ID(Ent-ID)","endKey").registerTempTable("orgmerge");

    }

    private Map getCfgMap(String path){
        Map<String,String> cfg = new HashMap<String, String>(3);
        cfg.put("header",defalt);
        cfg.put("path",path);
        cfg.put("delimiter", CommonConfig.getValue(Constants.ASSOCIATION_CSV_DELIMITER));
        return cfg;
    }

}
