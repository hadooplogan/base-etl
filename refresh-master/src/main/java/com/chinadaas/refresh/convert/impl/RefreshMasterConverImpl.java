package com.chinadaas.refresh.convert.impl;


import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.common.util.StringUtils;
import com.chinadaas.common.util.TimeUtil;
import com.chinadaas.refresh.etl.sql.RefreshMasterETL;
import io.netty.util.internal.StringUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Encode;

import java.util.*;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public class RefreshMasterConverImpl extends AbstractConvert{


    public void convertData(SparkSession spark, String date, String model, String allCfgPath, String incPath) {

        //刷库大师采用增量数据的方式获取数据
        Map<String,String> inccfg = MyFileUtil.getFileCfg(incPath);


        String id = date+"_" +TimeUtil.getNowStrYMD();
        String path = "/riskbell/master/"+id;

        RegisterTable.regiserEntBaseInfoTable(spark,"entbaseinfo",date,inccfg.get("ENTERPRISEBASEINFOCOLLECT"));
        RegisterTable.regiserGtEntBaseInfoTable(spark,"gtentbaseinfo",date,inccfg.get("E_GT_BASEINFO"));
        RegisterTable.regiserInvTable(spark,"e_inv_investment",date,inccfg.get("E_INV_INVESTMENT"));
        RegisterTable.regiserEpriPersonTable(spark,"e_pri_person",date,inccfg.get("E_PRI_PERSON"));

        Map cfg = new HashMap<String,String>();
        cfg.put(DatabaseValues.ES_NODES,"192.168.207.11,192.168.207.12,192.168.207.13");
        cfg.put(DatabaseValues.ES_PORT,"29200");
        cfg.put("es.mapping.id","pripid");
        cfg.put("es.resource.read","MASTER_ORDER/order");

        this.adapterES.loadData(spark,cfg).registerTempTable("order");

        this.adapterText.writeData(RefreshMasterETL.df2Text(spark),path);

        Map cfg2 = new HashMap<String,String>();
        cfg2.put(DatabaseValues.ES_NODES,"192.168.207.11,192.168.207.12,192.168.207.13");
        cfg2.put(DatabaseValues.ES_PORT,"29200");
        cfg2.put("es.mapping.id","id");
        cfg2.put("es.resource","master/master");

        this.adapterES.writeData(spark.createDataFrame(Arrays.asList(new EsStatus(id,date,"1","0","0")),EsStatus.class),cfg2);
    }



    public static  class EsStatus{
        private String id;
        private String date;
        private String hdfs;
        private String ftp;
        private String test;

        public String getId() {
            return id;
        }

        public EsStatus(String id, String date, String hdfs, String ftp, String test) {
            this.id = id;
            this.date = date;
            this.hdfs = hdfs;
            this.ftp = ftp;
            this.test = test;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getHdfs() {
            return hdfs;
        }

        public void setHdfs(String hdfs) {
            this.hdfs = hdfs;
        }

        public String getFtp() {
            return ftp;
        }

        public void setFtp(String ftp) {
            this.ftp = ftp;
        }

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }
    }
}
