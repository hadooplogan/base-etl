package com.chinadaas.association.etl.table;

import java.io.Serializable;

/**
 * Created by gongxs01 on 2017/12/19.
 */
public class MonitorIndex implements Serializable{

    private long hf_entcount;

    private long hf_invcount;

    private long hf_personcount;

    private long es_entcount;

    private long es_invcount;

    private long es_personcount;

    private long es_gtcount;

    private long es_gtperson;


    private String data_date;


    private String write_date;

    public MonitorIndex(long hf_entcount, long hf_invcount, long hf_personcount, long es_entcount,
                        long es_invcount, long es_personcount, long es_gtcount, long es_gtperson, String data_date, String write_date) {
        this.hf_entcount = hf_entcount;
        this.hf_invcount = hf_invcount;
        this.hf_personcount = hf_personcount;
        this.es_entcount = es_entcount;
        this.es_invcount = es_invcount;
        this.es_personcount = es_personcount;
        this.es_gtcount = es_gtcount;
        this.es_gtperson = es_gtperson;
        this.data_date = data_date;
        this.write_date = write_date;
    }

    public long getEs_gtcount() {
        return es_gtcount;
    }

    public void setEs_gtcount(long es_gtcount) {
        this.es_gtcount = es_gtcount;
    }

    public long getEs_gtperson() {
        return es_gtperson;
    }

    public void setEs_gtperson(long es_gtperson) {
        this.es_gtperson = es_gtperson;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getWrite_date() {
        return write_date;
    }

    public void setWrite_date(String write_date) {
        this.write_date = write_date;
    }

    public long getHf_entcount() {
        return hf_entcount;
    }

    public void setHf_entcount(long hf_entcount) {
        this.hf_entcount = hf_entcount;
    }

    public long getHf_invcount() {
        return hf_invcount;
    }

    public void setHf_invcount(long hf_invcount) {
        this.hf_invcount = hf_invcount;
    }

    public long getHf_personcount() {
        return hf_personcount;
    }

    public void setHf_personcount(long hf_personcount) {
        this.hf_personcount = hf_personcount;
    }

    public long getEs_entcount() {
        return es_entcount;
    }

    public void setEs_entcount(long es_entcount) {
        this.es_entcount = es_entcount;
    }

    public long getEs_invcount() {
        return es_invcount;
    }

    public void setEs_invcount(long es_invcount) {
        this.es_invcount = es_invcount;
    }

    public long getEs_personcount() {
        return es_personcount;
    }

    public void setEs_personcount(long es_personcount) {
        this.es_personcount = es_personcount;
    }
}
