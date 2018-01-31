package com.chinadaas.common.table;

/**
 * Created by gongxs01 on 2017/10/25.
 */
public class E_ALTER_RECODER {

    private String  s_ext_nodenum ;
    private String  pripid ;
    private String  s_ext_sequence ;
    private String  alt_id ;
    private String  altitem ;
    private String  altbe ;
    private String  altaf ;
    private String  altdate ;
    private String  alttime ;
    private String  s_ext_timestamp ;
    private String  s_ext_batch ;
    private String  s_ext_validflag;
    private String data_date;

    public E_ALTER_RECODER(String s_ext_nodenum, String pripid, String s_ext_sequence, String alt_id, String altitem, String altbe, String altaf, String altdate, String alttime, String s_ext_timestamp, String s_ext_batch, String s_ext_validflag, String data_date) {
        this.s_ext_nodenum = s_ext_nodenum;
        this.pripid = pripid;
        this.s_ext_sequence = s_ext_sequence;
        this.alt_id = alt_id;
        this.altitem = altitem;
        this.altbe = altbe;
        this.altaf = altaf;
        this.altdate = altdate;
        this.alttime = alttime;
        this.s_ext_timestamp = s_ext_timestamp;
        this.s_ext_batch = s_ext_batch;
        this.s_ext_validflag = s_ext_validflag;
        this.data_date = data_date;
    }

    @Override
    public String toString() {
        return "E_ALTER_RECODER{" +
                "s_ext_nodenum='" + s_ext_nodenum + '\'' +
                ", pripid='" + pripid + '\'' +
                ", s_ext_sequence='" + s_ext_sequence + '\'' +
                ", alt_id='" + alt_id + '\'' +
                ", altitem='" + altitem + '\'' +
                ", altbe='" + altbe + '\'' +
                ", altaf='" + altaf + '\'' +
                ", altdate='" + altdate + '\'' +
                ", alttime='" + alttime + '\'' +
                ", s_ext_timestamp='" + s_ext_timestamp + '\'' +
                ", s_ext_batch='" + s_ext_batch + '\'' +
                ", s_ext_validflag='" + s_ext_validflag + '\'' +
                '}';
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

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

    public String getS_ext_sequence() {
        return s_ext_sequence;
    }

    public void setS_ext_sequence(String s_ext_sequence) {
        this.s_ext_sequence = s_ext_sequence;
    }

    public String getAlt_id() {
        return alt_id;
    }

    public void setAlt_id(String alt_id) {
        this.alt_id = alt_id;
    }

    public String getAltitem() {
        return altitem;
    }

    public void setAltitem(String altitem) {
        this.altitem = altitem;
    }

    public String getAltbe() {
        return altbe;
    }

    public void setAltbe(String altbe) {
        this.altbe = altbe;
    }

    public String getAltaf() {
        return altaf;
    }

    public void setAltaf(String altaf) {
        this.altaf = altaf;
    }

    public String getAltdate() {
        return altdate;
    }

    public void setAltdate(String altdate) {
        this.altdate = altdate;
    }

    public String getAlttime() {
        return alttime;
    }

    public void setAlttime(String alttime) {
        this.alttime = alttime;
    }

    public String getS_ext_timestamp() {
        return s_ext_timestamp;
    }

    public void setS_ext_timestamp(String s_ext_timestamp) {
        this.s_ext_timestamp = s_ext_timestamp;
    }

    public String getS_ext_batch() {
        return s_ext_batch;
    }

    public void setS_ext_batch(String s_ext_batch) {
        this.s_ext_batch = s_ext_batch;
    }

    public String getS_ext_validflag() {
        return s_ext_validflag;
    }

    public void setS_ext_validflag(String s_ext_validflag) {
        this.s_ext_validflag = s_ext_validflag;
    }
}
