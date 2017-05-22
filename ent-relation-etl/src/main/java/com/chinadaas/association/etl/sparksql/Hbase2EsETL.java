package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;


/**
 * Created by gongxs01 on 2017/5/15.
 */
public class Hbase2EsETL implements Serializable{

    /**
     * ES个人节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonDataFrame(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT * FROM  e_pri_person_hdfs_ext_20170508 b ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personDataTmp");
    }

    /**
     * ES企业节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDataFrame(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" select \n" +
                "ENTNAME,\n" +
                "CREDIT_CODE,\n" +
                "REGNO,\n" +
                "ORIREGNO,\n" +
                "LICID,\n" +
                "name,\n" +
                "REGCAP,\n" +
                "REGCAPCUR,\n" +
                "ESDATE,\n" +
                "OPFROM,\n" +
                "OPTO,\n" +
                "ENTTYPE,\n" +
                "ENTSTATUS,\n" +
                "ABUITEM,\n" +
                "OPSCOPE,\n" +
                "REGORG,\n" +
                "S_EXT_NODENUM,\n" +
                "ANCHEYEAR,\n" +
                "CANDATE,\n" +
                "REVDATE,\n" +
                "RECCAP,\n" +
                "APPRDATE,\n" +
                "ENTCAT,\n" +
                "DOM,\n" +
                "DOMDISTRICT\n" +
                "from enterprisebaseinfocollect_hdfs_ext_20170515 ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "entDataInfoTmp");
    }

    /**
     * ES投资数据
     * @param sqlContext
     * @return
     */
    public DataFrame getinvDataFrame(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT * FROM  e_inv_investment_hdfs_ext_20170508 b ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "invDataTmp");
    }
}
