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
LOCATION '/tmp/ent-relation/tmp/dstpath/ent';
--��Ա�ڵ�
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string,
      encode_v1 string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/person';
--��ҵͬһ��ַ�ڵ�
CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/eaddr';

--��ҵͬһ�绰�ڵ�
CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/etel';
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
LOCATION '/tmp/ent-relation/tmp/dstpath/personinv';
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
LOCATION '/tmp/ent-relation/tmp/dstpath/entinv';
--���˹�ϵ
CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/legal';

--ְλ��ϵ
CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/staff';
--��ҵͬһ��ַ����
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entaddr';
--��ҵͬһ�绰����
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/enttel';
--��Աͬһ��ַ����
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/peraddr';
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
LOCATION '/tmp/ent-relation/tmp/dstpath/invhold';
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
LOCATION '/tmp/ent-relation/tmp/dstpath/invjoin';
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
LOCATION '/tmp/ent-relation/tmp/dstpath/personjoin';

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
LOCATION '/tmp/ent-relation/tmp/dstpath/personhold';

--��ҵͬһ�绰��ϵ��������
CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entandenttel';
--��ҵͬһ��ַ��������
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/entandentaddr';
--��Ա��ϵ�ϲ�
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/personmerge';
--��ҵ��ϵ�ϲ�
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/tmp/ent-relation/tmp/dstpath/invmerge';
--��֯����
CREATE EXTERNAL TABLE entorg_gxs(
     key string,
      name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/ent-relation/tmp/dstpath/org';
--��֯�ṹͶ��
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

--��֯�ṹ�عɹ�ϵ
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


--��ҵ�ڵ�
DROP TABLE entbaseinfo_gxs;
--��Ա�ڵ�
DROP TABLE person_gxs;
--��ҵͬһ��ַ�ڵ�
DROP TABLE pri_ent_addr_gxs;
--��ҵͬһ�绰�ڵ�
DROP TABLE pri_ent_tel_gxs;
--��ԱͶ�ʹ�ϵ
DROP TABLE person_inv_relation_gxs;
--��ҵͶ�ʹ�ϵ
DROP TABLE ent_inv_relation_gxs;
--���˹�ϵ
DROP TABLE lerepsign_relation_gxs;
--ְλ��ϵ
DROP TABLE position_relation_gxs;
--��ҵͬһ��ַ����
DROP TABLE pri_ent_addr_relation_gxs;
--��ҵͬһ�绰����
DROP TABLE pri_ent_tel_relation_gxs;
--��Աͬһ��ַ����
DROP TABLE pri_person_addr_relation_gxs;
--��ҵ�عɹ�ϵ
DROP TABLE ent_invhold_relation_gxs;
--��ҵ�ιɹ�ϵ
DROP TABLE ent_invjoin_relation_gxs;
--��Ա�ιɹ�ϵ
DROP TABLE person_join_relation_gxs;

--��Ա�عɹ�ϵ
DROP TABLE person_hold_relation_gxs;

--��ҵͬһ�绰��ϵ��������
DROP TABLE ent_andenttel_relation_gxs;
--��ҵͬһ��ַ��������
DROP TABLE ent_andentaddr_relation_gxs;
--��Ա��ϵ�ϲ�
DROP TABLE person_personmerge_relation_gxs;
--��ҵ��ϵ�ϲ�
DROP TABLE ent_invmerge_relation_gxs;
--��֯����
DROP TABLE entorg_gxs;
--��֯�ṹͶ��
DROP TABLE ent_orginv_relation_gxs;
--��֯�ṹ�عɹ�ϵ
DROP TABLE org_hold_relation_gxs;