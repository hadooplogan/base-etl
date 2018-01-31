package com.chinadaas.preprocess.etl.conver.impl;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.IDataAdapter;
import com.chinadaas.common.load.impl.CsvDataAdapterImpl;
import com.chinadaas.common.load.impl.HdfsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.preprocess.etl.conver.IConvert;
import com.chinadaas.preprocess.etl.sql.LegalStaffETL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public abstract class AbstractConvert implements IConvert {


    final IDataAdapter<Dataset,SparkSession,Map<String,String>>  adapter = new CsvDataAdapterImpl();

    final IDataAdapter<Dataset,SparkSession,String>  adapterParquet = new ParquetDataAdapterImpl();

    final IDataAdapter<Dataset,SparkSession,String>  adapterHdfs = new HdfsDataAdapterImpl();


    final static String DST_PATH = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_DSTPATH_TMP);


    protected void registerTableTmp(SparkSession spark,String date,String model, Map<String,String> cfg){

        if("all".equals(model)){
            RegisterTable.regiserEntBaseInfoTable(spark,"entbaseinfo_all",date,cfg.get("ENTERPRISEBASEINFOCOLLECT"));
            RegisterTable.regiserInvTable(spark,"e_inv_investment_all",date,cfg.get("E_INV_INVESTMENT"));
            RegisterTable.regiserEpriPersonTable(spark,"e_pri_person_all",date,cfg.get("E_PRI_PERSON"));

        }else if("inc".equals(model)){
            RegisterTable.regiserEntBaseInfoTable(spark,"entbaseinfo_inc",date, Constants.ENT_BASEINFO_PATH+"20171211");
            RegisterTable.regiserInvTable(spark,"e_inv_investment_inc",date,Constants.E_INV_INVESTMENT_PATH+"20171211");
            RegisterTable.regiserEpriPersonTable(spark,"e_pri_person_inc",date,Constants.E_PRI_PERSON_PATH+"20171211");

            RegisterTable.regiserEntBaseInfoTable(spark,"entbaseinfo_del",date, Constants.ENT_BASEINFO_PATH_DEL+"20171128");
            RegisterTable.regiserInvTable(spark,"e_inv_investment_del",date,Constants.E_INV_INVESTMENT_PATH_DEL+"20171128");
            RegisterTable.regiserEpriPersonTable(spark,"e_pri_person_del",date,Constants.E_PRI_PERSON_PATH_DEL+"20171128");
        }

    }

}
