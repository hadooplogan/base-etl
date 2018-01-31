package com.chinadaas.next.etl.sparksql.convert.impl;

import com.chinadaas.next.etl.sparksql.sql.ChangeKpiETL;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/21.
 */
public class ChangeCovertImpl extends AbstractConvert{
    @Override
    public void convertData(SparkSession spark, String date) {

        this.registerTableTmp(spark);
        adapterParquet.writeData(ChangeKpiETL.getChangeKpiDF(spark,date),"/next/ent_index/change");

    }
}
