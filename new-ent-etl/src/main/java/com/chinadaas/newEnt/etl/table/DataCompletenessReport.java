package com.chinadaas.newEnt.etl.table;

/**
 *
 * Created by 74061 on 2017/7/3.
 */
public class DataCompletenessReport {

    private String regorg;
    private String diffDate;
    private String dataCou;

    public String getRegorg() {
        return regorg;
    }
    public void setRegorg(String regorg) {
        this.regorg = regorg;
    }

    public String getDataCou() {
        return dataCou;
    }
    public void setDataCou(String dataCou) {
        this.dataCou = dataCou;
    }

    public String getDiffDate() {
        return diffDate;
    }
    public void setDiffDate(String diffDate) {
        this.diffDate = diffDate;
    }

    public DataCompletenessReport(String regorg, String diffDate,String dataCou) {
        super();
        this.regorg = regorg;
        this.diffDate = diffDate;
        this.dataCou = dataCou;
    }
    public DataCompletenessReport() {
        super();
    }

 }
