package com.chinadaas.newEnt.etl.table;

import java.io.Serializable;

public class DataQuality implements Serializable{
	 
	private static final long serialVersionUID = 1L;
	
	private Integer regnoInvalid;//注册号无效
	private Integer regnoInvalidRate;//注册号无效数占比
	
	private Integer creditCodeInvalid;//统一信用代码无效数
	private Integer creditCodeInvalidRate;//统一信用代码无效数占比
	
	private Integer telInvalid;//电话无效数
	private Integer telInvalidRate;//电话无效数占比
	
	private Integer nameInvalid;//法人无效数
    private Integer nameInvalidRate;//法人无效数占比
	 
    private Integer invInvalid;//股东无效数
    private Integer invInvalidRate;//股东无效数占比
	
    private Integer industryphyInvalid;//行业门类无效数
    private Integer industryphyInvalidRate;//行业无效数占比
    
    private Integer industrycoInvalid;//行业小类无效数
    private Integer industrycoInvalidRate;//行业小类无效数占比
    
    private Integer domInvalid;//地址无效数
    private Integer domInvalidRate;//地址无效数占比
    
	 
	/*@Override
	public String toString() {
		return "Person [id=" + id + ", name=" + name + ", age=" + age + "]";
	}*/
	public DataQuality(Integer regnoInvalid, Integer regnoInvalidRate,
	        Integer creditCodeInvalid, Integer creditCodeInvalidRate,
	        Integer telInvalid, Integer telInvalidRate,
	        Integer nameInvalid, Integer nameInvalidRate,
	        Integer invInvalid, Integer invInvalidRate,
	        Integer industryphyInvalid, Integer industryphyInvalidRate,
	        Integer industrycoInvalid, Integer industrycoInvalidRate,
	        Integer domInvalid, Integer domInvalidRate) {
		super();
		this.regnoInvalid = regnoInvalid;
		this.regnoInvalidRate = regnoInvalidRate;
		this.creditCodeInvalid = creditCodeInvalid;
		this.creditCodeInvalidRate = creditCodeInvalidRate;
		this.telInvalid = telInvalid;
		this.telInvalidRate = telInvalidRate;
		this.nameInvalid = nameInvalid;
		this.nameInvalidRate = nameInvalidRate;
		this.invInvalid = invInvalid;
		this.invInvalidRate = invInvalidRate;
		this.industryphyInvalid = industryphyInvalid;
		this.industryphyInvalidRate = industryphyInvalidRate;
		this.industrycoInvalid = industrycoInvalid;
		this.industrycoInvalidRate = industrycoInvalidRate;
		this.domInvalid = domInvalid;
		this.domInvalidRate = domInvalidRate;
		
	}
	
	public DataQuality(Integer regnoInvalid, Integer creditCodeInvalid, Integer telInvalid,Integer nameInvalid, 
            Integer industryphyInvalid, Integer industrycoInvalid, Integer domInvalid ,Integer invInvalid ) {
        super();
        this.regnoInvalid = regnoInvalid;
        this.creditCodeInvalid = creditCodeInvalid;
        this.telInvalid = telInvalid;
        this.nameInvalid = nameInvalid;
        this.invInvalid = invInvalid;
        this.industryphyInvalid = industryphyInvalid;
        this.industrycoInvalid = industrycoInvalid;
        this.domInvalid = domInvalid;
        
    }
	public Integer getRegnoInvalid() {
        return regnoInvalid;
    }

    public void setRegnoInvalid(Integer regnoInvalid) {
        this.regnoInvalid = regnoInvalid;
    }

    public Integer getRegnoInvalidRate() {
        return regnoInvalidRate;
    }

    public void setRegnoInvalidRate(Integer regnoInvalidRate) {
        this.regnoInvalidRate = regnoInvalidRate;
    }

    public Integer getCreditCodeInvalid() {
        return creditCodeInvalid;
    }

    public void setCreditCodeInvalid(Integer creditCodeInvalid) {
        this.creditCodeInvalid = creditCodeInvalid;
    }

    public Integer getCreditCodeInvalidRate() {
        return creditCodeInvalidRate;
    }

    public void setCreditCodeInvalidRate(Integer creditCodeInvalidRate) {
        this.creditCodeInvalidRate = creditCodeInvalidRate;
    }

    public Integer getTelInvalid() {
        return telInvalid;
    }

    public void setTelInvalid(Integer telInvalid) {
        this.telInvalid = telInvalid;
    }

    public Integer getTelInvalidRate() {
        return telInvalidRate;
    }

    public void setTelInvalidRate(Integer telInvalidRate) {
        this.telInvalidRate = telInvalidRate;
    }

    public Integer getNameInvalid() {
        return nameInvalid;
    }

    public void setNameInvalid(Integer nameInvalid) {
        this.nameInvalid = nameInvalid;
    }

    public Integer getNameInvalidRate() {
        return nameInvalidRate;
    }

    public void setNameInvalidRate(Integer nameInvalidRate) {
        this.nameInvalidRate = nameInvalidRate;
    }

    public Integer getInvInvalid() {
        return invInvalid;
    }


    public void setInvInvalid(Integer invInvalid) {
        this.invInvalid = invInvalid;
    }


    public Integer getInvInvalidRate() {
        return invInvalidRate;
    }


    public void setInvInvalidRate(Integer invInvalidRate) {
        this.invInvalidRate = invInvalidRate;
    }


    public Integer getIndustryphyInvalid() {
        return industryphyInvalid;
    }


    public void setIndustryphyInvalid(Integer industryphyInvalid) {
        this.industryphyInvalid = industryphyInvalid;
    }


    public Integer getIndustryphyInvalidRate() {
        return industryphyInvalidRate;
    }


    public void setIndustryphyInvalidRate(Integer industryphyInvalidRate) {
        this.industryphyInvalidRate = industryphyInvalidRate;
    }


    public Integer getIndustrycoInvalid() {
        return industrycoInvalid;
    }


    public void setIndustrycoInvalid(Integer industrycoInvalid) {
        this.industrycoInvalid = industrycoInvalid;
    }


    public Integer getIndustrycoInvalidRate() {
        return industrycoInvalidRate;
    }


    public void setIndustrycoInvalidRate(Integer industrycoInvalidRate) {
        this.industrycoInvalidRate = industrycoInvalidRate;
    }


    public Integer getDomInvalid() {
        return domInvalid;
    }


    public void setDomInvalid(Integer domInvalid) {
        this.domInvalid = domInvalid;
    }


    public Integer getDomInvalidRate() {
        return domInvalidRate;
    }


    public void setDomInvalidRate(Integer domInvalidRate) {
        this.domInvalidRate = domInvalidRate;
    }


    public static long getSerialversionuid() {
		return serialVersionUID;
	}
	 
	 
}