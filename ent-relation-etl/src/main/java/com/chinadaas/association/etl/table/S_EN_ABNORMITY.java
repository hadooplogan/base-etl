package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2017/9/15.
 * 经营异常名录
 */
public class S_EN_ABNORMITY {

    private String entname;

    private String indate;

    private String outdate;

    private String inreason;

    private String outreason;

    private String pripid;

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getEntname() {
        return entname;
    }

    public void setEntname(String entname) {
        this.entname = entname;
    }

    public String getIndate() {
        return indate;
    }

    public void setIndate(String indate) {
        this.indate = indate;
    }

    public String getOutdate() {
        return outdate;
    }

    public void setOutdate(String outdate) {
        this.outdate = outdate;
    }

    public String getInreason() {
        return inreason;
    }

    public void setInreason(String inreason) {
        this.inreason = inreason;
    }

    public String getOutreason() {
        return outreason;
    }

    public void setOutreason(String outreason) {
        this.outreason = outreason;
    }


}
