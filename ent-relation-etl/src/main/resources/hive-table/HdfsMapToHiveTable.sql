--��ҵ�ڵ�
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
--��Ա�ڵ�
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/person';
--��ҵͬһ��ַ�ڵ�
CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/tmp/hive_export_inv/tmp/dstpath/eaddr';

--��ҵͬһ�绰�ڵ�
CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/etel';
--��ԱͶ�ʹ�ϵ
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
--��ҵͶ�ʹ�ϵ
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
--���˹�ϵ
CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/legal';

--ְλ��ϵ
CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/staff';
--��ҵͬһ��ַ����
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entaddr';
--��ҵͬһ�绰����
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/enttel';
--��Աͬһ��ַ����
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/peraddr';
--��ҵ�عɹ�ϵ
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
--��ҵ�ιɹ�ϵ
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
--��Ա�ιɹ�ϵ
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

--��Ա�عɹ�ϵ
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

--��ҵͬһ�绰��ϵ��������
CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entandenttel';
--��ҵͬһ��ַ��������
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/entandentaddr';
--��Ա��ϵ�ϲ�
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/personmerge';
--��ҵ��ϵ�ϲ�
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/hive_export_inv/tmp/dstpath/invmerge';

