package com.chinadaas.common.load;



/**
 * Created by gongxs01 on 2017/9/5.
 */
public interface IDataAdapter<R,P,C> {

     R loadData(P spark,C params);

     void writeData(R ds, C path);
}
