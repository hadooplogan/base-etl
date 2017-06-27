package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.ILoadInterface;
import org.apache.spark.sql.DataFrame;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gongxs01 on 2017/6/12.
 */
public class LoadOracleImpl implements ILoadInterface<DataFrame,Map<String, Object>> {

    public  DataFrame loadDT(Map<String, Object> params) {

        return loadOracleDT(params);
    }

    private static DataFrame loadOracleDT(Map<String, Object> params){
/*        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:oracle:thin:DM_RISKBELL/DM_RISKBELL@192.168.205.30:1521/orcl");
        options.put("driver", "oracle.jdbc.OracleDriver");
        options.put("dbtable", params.get("tableName").toString());

        return sqlContext.read().format("jdbc").options(options).load();*/

        return null;
    };

}
