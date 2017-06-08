package com.chinadaas.association.etl.table;

import java.io.Serializable;

/**
 * Created by gongxs01 on 2017/6/6.
 */
public class InvEO implements Serializable {

    private static final long serialVersionUID = 1025026912977591750L;

    public InvEO() {

    }

    public InvEO(String s_ext_nodenum, String pripid, String invid, String inv, String invtype
            , String certype, String cerno, String blictype, String blicno, String country, String currency,
                 String subconam, String acconam, String conprop, String conform, String condate,
                 String conam, String cerno_old, String zspid) {
        this.s_ext_nodenum = s_ext_nodenum;
        this.pripid = pripid;
        this.invid = invid;
        this.inv = inv;
        this.invtype = invtype;
        this.certype = certype;
        this.cerno = cerno;
        this.blictype = blictype;
        this.blicno = blicno;
        this.country = country;
        this.currency = currency;
        this.subconam = subconam;
        this.acconam = acconam;
        this.conprop = conprop;
        this.conform = conform;
        this.condate = condate;
        this.conam = conam;
        this.cerno_old = cerno_old;
        this.zspid = zspid;
    }

    private String s_ext_nodenum;
    private String pripid;
    private String invid;
    private String inv;
    private String invtype;
    private String certype;
    private String cerno;
    private String blictype;
    private String blicno;
    private String country;
    private String currency;
    private String subconam;
    private String acconam;
    private String conprop;
    private String conform;
    private String condate;
    private String conam;
    private String cerno_old;
    private String zspid;

    public String getS_ext_nodenum() {
        return s_ext_nodenum;
    }

    public void setS_ext_nodenum(String s_ext_nodenum) {
        this.s_ext_nodenum = s_ext_nodenum;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getInvid() {
        return invid;
    }

    public void setInvid(String invid) {
        this.invid = invid;
    }

    public String getInv() {
        return inv;
    }

    public void setInv(String inv) {
        this.inv = inv;
    }

    public String getInvtype() {
        return invtype;
    }

    public void setInvtype(String invtype) {
        this.invtype = invtype;
    }

    public String getCertype() {
        return certype;
    }

    public void setCertype(String certype) {
        this.certype = certype;
    }

    public String getCerno() {
        return cerno;
    }

    public void setCerno(String cerno) {
        this.cerno = cerno;
    }

    public String getBlictype() {
        return blictype;
    }

    public void setBlictype(String blictype) {
        this.blictype = blictype;
    }

    public String getBlicno() {
        return blicno;
    }

    public void setBlicno(String blicno) {
        this.blicno = blicno;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getSubconam() {
        return subconam;
    }

    public void setSubconam(String subconam) {
        this.subconam = subconam;
    }

    public String getAcconam() {
        return acconam;
    }

    public void setAcconam(String acconam) {
        this.acconam = acconam;
    }

    public String getConprop() {
        return conprop;
    }

    public void setConprop(String conprop) {
        this.conprop = conprop;
    }

    public String getConform() {
        return conform;
    }

    public void setConform(String conform) {
        this.conform = conform;
    }

    public String getCondate() {
        return condate;
    }

    public void setCondate(String condate) {
        this.condate = condate;
    }

    public String getConam() {
        return conam;
    }

    public void setConam(String conam) {
        this.conam = conam;
    }

    public String getCerno_old() {
        return cerno_old;
    }

    public void setCerno_old(String cerno_old) {
        this.cerno_old = cerno_old;
    }

    public String getZspid() {
        return zspid;
    }

    public void setZspid(String zspid) {
        this.zspid = zspid;
    }

}
