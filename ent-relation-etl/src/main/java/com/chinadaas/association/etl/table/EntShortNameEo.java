package com.chinadaas.association.etl.table;

/**
 * Created by gongxs01 on 2017/7/21.
 */
public class EntShortNameEo {
    private String entName;
    private String shortName;

    public String getEntName() {
        return entName;
    }

    public  EntShortNameEo(){

    }

    public  EntShortNameEo(String entName,String shortName){
        this.entName=entName;
        this.shortName=shortName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }
}
