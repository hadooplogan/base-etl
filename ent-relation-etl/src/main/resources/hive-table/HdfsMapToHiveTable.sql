--企业节点
CREATE EXTERNAL TABLE entbaseinfo_gxs(
      pripid string,
      entname string,
      regno string,
      credit_code string,
      esdate string,
      industryphy string,
      regcap string,
      entstatus string,
      regcapcur string,
      riskinfo string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/ent';
--人员节点
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string,
      encode_v1 string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/person';
--企业同一地址节点
CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/eaddr';

--企业同一电话节点
CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/etel';
--人员投资关系
CREATE EXTERNAL TABLE person_inv_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/personinv';
--企业投资关系
CREATE EXTERNAL TABLE ent_inv_relation_gxs(
      pripid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entinv';
--法人关系
CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/legal';

--职位关系
CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/staff';
--企业同一地址关联
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entaddr';
--企业同一电话关联
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/enttel';
--人员同一地址关联
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/peraddr';
--企业控股关系
CREATE EXTERNAL TABLE ent_invhold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/invhold';
--企业参股关系
CREATE EXTERNAL TABLE ent_invjoin_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/invjoin';
--人员参股关系
CREATE EXTERNAL TABLE person_join_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/personjoin';

--人员控股关系
CREATE EXTERNAL TABLE person_hold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/personhold';

--企业同一电话关系风险评分
CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entandenttel';
--企业同一地址风险评分
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entandentaddr';
--人员关系合并
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/personmerge';
--企业关系合并
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/invmerge';
--组织机构
CREATE EXTERNAL TABLE entorg_gxs(
     key string,
      name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/org';
--组织结构投资
CREATE EXTERNAL TABLE ent_orginv_relation_gxs(
      pripid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/orginv';

--组织结构控股关系
CREATE EXTERNAL TABLE org_hold_relation_gxs(
      zspid string,
      condate string,
      subconam string,
      currency string,
      conprop string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/orghold';


--企业节点
DROP TABLE entbaseinfo_gxs;
--人员节点
DROP TABLE person_gxs;
--企业同一地址节点
DROP TABLE pri_ent_addr_gxs;
--企业同一电话节点
DROP TABLE pri_ent_tel_gxs;
--人员投资关系
DROP TABLE person_inv_relation_gxs;
--企业投资关系
DROP TABLE ent_inv_relation_gxs;
--法人关系
DROP TABLE lerepsign_relation_gxs;
--职位关系
DROP TABLE position_relation_gxs;
--企业同一地址关联
DROP TABLE pri_ent_addr_relation_gxs;
--企业同一电话关联
DROP TABLE pri_ent_tel_relation_gxs;
--人员同一地址关联
DROP TABLE pri_person_addr_relation_gxs;
--企业控股关系
DROP TABLE ent_invhold_relation_gxs;
--企业参股关系
DROP TABLE ent_invjoin_relation_gxs;
--人员参股关系
DROP TABLE person_join_relation_gxs;

--人员控股关系
DROP TABLE person_hold_relation_gxs;

--企业同一电话关系风险评分
DROP TABLE ent_andenttel_relation_gxs;
--企业同一地址风险评分
DROP TABLE ent_andentaddr_relation_gxs;
--人员关系合并
DROP TABLE person_personmerge_relation_gxs;
--企业关系合并
DROP TABLE ent_invmerge_relation_gxs;
--组织机构
DROP TABLE entorg_gxs;
--组织结构投资
DROP TABLE ent_orginv_relation_gxs;
--组织结构控股关系
DROP TABLE org_hold_relation_gxs;