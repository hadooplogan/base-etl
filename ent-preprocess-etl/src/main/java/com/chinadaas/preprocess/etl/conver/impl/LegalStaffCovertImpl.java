package com.chinadaas.preprocess.etl.conver.impl;


import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.preprocess.etl.sql.LegalStaffETL;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/9/21.
 */
public class LegalStaffCovertImpl extends AbstractConvert{
    @Override
    public void convertData(SparkSession spark, String[] args) {


        String date= args[0];
        String model = args[1];

        Map<String,String> cfg = MyFileUtil.getFileCfg(args[2]);

        this.registerTableTmp(spark,date,model,cfg);

        if("all".equals(model)){

            LegalStaffETL.getLegalStaff(spark,"all").registerTempTable("all");

            adapterParquet.writeData(spark.sql("select *,'all' as model from all "),"/tmp/spark_test/staff/data_date="+date);

        }else if("inc".equals(model)){

            LegalStaffETL.getLegalStaff(spark,"inc").registerTempTable("inc");

            LegalStaffETL.getLegalStaff(spark,"del").registerTempTable("del");

            adapterParquet.writeData(spark.sql("select *,'inc' as model from inc union select *,'del' as model from del "),"/tmp/spark_test/staff/data_date="+date);
        }
    }

}
