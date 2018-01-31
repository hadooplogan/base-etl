package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2018/1/5.
 */
public class LeafNodeRelation {

    private String orgid;

    private String pripid;

    private String conprop;

    public LeafNodeRelation(String orgid, String pripid, String conprop) {
        this.orgid = orgid;
        this.pripid = pripid;
        this.conprop = conprop;
    }

    public String getOrgid() {
        return orgid;
    }

    public void setOrgid(String orgid) {
        this.orgid = orgid;
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
}
