package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2017/9/15.
 * 税收违法案件
 */
public class S_TAX_PUNISH {

    private String tax_name;

    private String leg_detail;

    private String fina_detail;

    private String case_type;

    private String pripid;

    public String getTax_name() {
        return tax_name;
    }

    public void setTax_name(String tax_name) {
        this.tax_name = tax_name;
    }

    public String getLeg_detail() {
        return leg_detail;
    }

    public void setLeg_detail(String leg_detail) {
        this.leg_detail = leg_detail;
    }

    public String getFina_detail() {
        return fina_detail;
    }

    public void setFina_detail(String fina_detail) {
        this.fina_detail = fina_detail;
    }

    public String getCase_type() {
        return case_type;
    }

    public void setCase_type(String case_type) {
        this.case_type = case_type;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }
}
