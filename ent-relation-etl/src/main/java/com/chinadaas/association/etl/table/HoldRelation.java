package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2017/12/14.
 */
public class HoldRelation {
    private String pripid;
    private String condate;
    private String subconam;
    private String currency;
    private String conprop;
    private String topripid;

    public HoldRelation(String pripid, String condate, String subconam, String currency, String conprop, String topripid) {
        this.pripid = pripid;
        this.condate = condate;
        this.subconam = subconam;
        this.currency = currency;
        this.conprop = conprop;
        this.topripid = topripid;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getCondate() {
        return condate;
    }

    public void setCondate(String condate) {
        this.condate = condate;
    }

    public String getSubconam() {
        return subconam;
    }

    public void setSubconam(String subconam) {
        this.subconam = subconam;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getConprop() {
        return conprop;
    }

    public void setConprop(String conprop) {
        this.conprop = conprop;
    }

    public String getTopripid() {
        return topripid;
    }

    public void setTopripid(String topripid) {
        this.topripid = topripid;
    }
}
