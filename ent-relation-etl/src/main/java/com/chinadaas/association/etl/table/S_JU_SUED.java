package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2017/9/15.
 *
 * 司法诉讼信息表
 */
public class S_JU_SUED {

    private String fss_status;

    private String fss_enfcourt;

    private String fss_money;

    private String fss_name;

    private String fss_time;

    private String zspid;

    public String getZspid() {
        return zspid;
    }

    public void setZspid(String zspid) {
        this.zspid = zspid;
    }

    public String getFss_status() {
        return fss_status;
    }

    public void setFss_status(String fss_status) {
        this.fss_status = fss_status;
    }

    public String getFss_enfcourt() {
        return fss_enfcourt;
    }

    public void setFss_enfcourt(String fss_enfcourt) {
        this.fss_enfcourt = fss_enfcourt;
    }

    public String getFss_money() {
        return fss_money;
    }

    public void setFss_money(String fss_money) {
        this.fss_money = fss_money;
    }

    public String getFss_name() {
        return fss_name;
    }

    public void setFss_name(String fss_name) {
        this.fss_name = fss_name;
    }

    public String getFss_time() {
        return fss_time;
    }

    public void setFss_time(String fss_time) {
        this.fss_time = fss_time;
    }
}
