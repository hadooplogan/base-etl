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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/ent';
--人员节点
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/person';
--企业同一地址节点
CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/tmp/hive_export_inv/tmp/dstpath/eaddr';

--企业同一电话节点
CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/etel';
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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/personinv';
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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entinv';
--法人关系
CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/legal';

--职位关系
CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/staff';
--企业同一地址关联
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entaddr';
--企业同一电话关联
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/enttel';
--人员同一地址关联
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/peraddr';
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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/invhold';
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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/invjoin';
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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/personjoin';

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
LOCATION '/tmp/hive_export_inv/tmp/dstpath/personhold';

--企业同一电话关系风险评分
CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entandenttel';
--企业同一地址风险评分
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entandentaddr';
--人员关系合并
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/personmerge';
--企业关系合并
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/invmerge';

