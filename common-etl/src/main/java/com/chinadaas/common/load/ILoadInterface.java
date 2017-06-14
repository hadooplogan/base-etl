package com.chinadaas.common.load;

import org.apache.spark.sql.DataFrame;

/**
 * Created by gongxs01 on 2017/6/12.
 */
public interface ILoadInterface<R,P> {
   public  R loadDT(P params);
}
