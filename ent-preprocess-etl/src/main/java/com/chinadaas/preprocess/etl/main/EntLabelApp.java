package com.chinadaas.preprocess.etl.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.preprocess.etl.sql.SaveLabelAsText;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 光大银行企业标签
 */
public class EntLabelApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                enableHiveSupport().
                appName("ent-label-process").
                getOrCreate();

        ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();

        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BASE_DIR)).createOrReplaceTempView("base");//基本标签
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ABNORMITY_DIR)).createOrReplaceTempView("abnormity");//经营异常
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BREAKLAW_DIR)).createOrReplaceTempView("breaklaw");//严重违法
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_CASEINFO_DIR)).createOrReplaceTempView("caseinfo");//行政处罚
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALOFFICE_DIR)).createOrReplaceTempView("legaloffice");//法人任职
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISEINVESTMENT_DIR)).createOrReplaceTempView("entinvestment");//企业对外投资
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALINVESTMENT_DIR)).createOrReplaceTempView("legalinvestment");//法人对外投资
       // pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISE_EXCHANGE_DIR)).createOrReplaceTempView("entchange");//企业变更信息
        pda.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_STOCKOFCOMPANY_DIR)).createOrReplaceTempView("stockofcompany");



        DataFrameUtil.saveAsTextFileOverwrite(SaveLabelAsText.getentlabel(spark), "/next/guangdatest/");

    }


}
