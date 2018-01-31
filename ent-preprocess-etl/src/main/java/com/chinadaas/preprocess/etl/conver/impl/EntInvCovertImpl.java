package com.chinadaas.preprocess.etl.conver.impl;

import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.preprocess.etl.sql.EntInvETL;
import com.chinadaas.preprocess.etl.sql.LegalInvETL;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/12/4.
 */
public class EntInvCovertImpl  extends AbstractConvert{

    @Override
    public void convertData(SparkSession spark, String[] args) {
        String date= args[0];
        String model = args[1];

        Map<String,String> cfg = MyFileUtil.getFileCfg(args[2]);

        this.registerTableTmp(spark,date,model,cfg);

        if("all".equals(model)){
            EntInvETL.getEntInvChange(spark,"all").registerTempTable("all");
            adapterParquet.writeData(spark.sql("select *,'all' as model from all "),"/tmp/spark_test/entinv");
        }else if("inc".equals(model)){
            EntInvETL.getEntInvChange(spark,"inc").registerTempTable("inc");
            EntInvETL.getEntInvChange(spark,"del").registerTempTable("del");
            adapterParquet.writeData(spark.sql("select *,'inc' as model from inc union select *,'del' as model from del "),
                    "/tmp/spark_test/entinv");
        }
    }
}
