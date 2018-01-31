package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2018/1/5.
 */
public class EntInvRelation {

    private String pripid;

    private String conprop;

    private String invid;

    public EntInvRelation(String pripid, String conprop, String invid) {
        this.pripid = pripid;
        this.conprop = conprop;
        this.invid = invid;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getConprop() {
        return conprop;
    }

    public void setConprop(String conprop) {
        this.conprop = conprop;
    }

    public String getInvid() {
        return invid;
    }

    public void setInvid(String invid) {
        this.invid = invid;
    }
}
