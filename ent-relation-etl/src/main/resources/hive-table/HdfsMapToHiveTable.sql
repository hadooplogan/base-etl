--��ҵ�ڵ�
CREATE EXTERNAL TABLE dev1_relation_entbaseinfo_gxs(
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
FIELDS TERMINATED BY '|';
LOCATION '/relation/dstpath/relation_data20170831/ent';

load data inpath '/relation/dstpath/relation_data20171211/ent' into table dev1_relation_entbaseinfo_gxs;
--��Ա�ڵ�
CREATE EXTERNAL TABLE person_gxs(
      zsid string,
      name string,     
      riskinfo string,
      encode_v1 string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/person';

--��ҵͬһ��ַ�ڵ�
CREATE EXTERNAL TABLE pri_ent_addr_gxs(
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/eaddr';

--��ҵͬһ�绰�ڵ�
CREATE EXTERNAL TABLE pri_ent_tel_gxs(
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/etel';
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
LOCATION '/relation/dstpath/relation/personinv';
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
LOCATION '/relation/dstpath/relation_data20170913/entinv';


--���˹�ϵ
CREATE EXTERNAL TABLE lerepsign_relation_gxs(
      personid string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/legal';

--ְλ��ϵ
CREATE EXTERNAL TABLE position_relation_gxs(
      personid string,
      position string,
      pripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/staff';
--��ҵͬһ��ַ����
CREATE EXTERNAL TABLE pri_ent_addr_relation_gxs(
      pripid string,
      dom string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entaddr';
--��ҵͬһ�绰����
CREATE EXTERNAL TABLE pri_ent_tel_relation_gxs(
      pripid string,
      tel string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/enttel';
--��Աͬһ��ַ����
CREATE EXTERNAL TABLE pri_person_addr_relation_gxs(
      personid string,
      addr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/peraddr';
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
LOCATION '/relation/dstpath/relation/invhold';
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
LOCATION '/relation/dstpath/relation/invjoin';
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
LOCATION '/relation/dstpath/relation/personjoin';

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
LOCATION '/relation/dstpath/relation/personhold';

--��ҵͬһ�绰��ϵ��������
CREATE EXTERNAL TABLE ent_andenttel_relation_gxs(
      pripid string,
      tel string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entandenttel';
--��ҵͬһ��ַ��������
CREATE EXTERNAL TABLE ent_andentaddr_relation_gxs(
      pripid string,
      dom string,
      topripid string,
      riskscore string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/entandentaddr';
--��Ա��ϵ�ϲ�
CREATE EXTERNAL TABLE person_personmerge_relation_gxs(
      pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/personmerge';
--��ҵ��ϵ�ϲ�
CREATE EXTERNAL TABLE ent_invmerge_relation_gxs(
     pripid string,
      riskscore string,
      topripid string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION '/relation/dstpath/relation/invmerge';
--��֯����
CREATE EXTERNAL TABLE entorg_gxs(
     key string,
      name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/relation/dstpath/relation/org';
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
LOCATION '/relation/dstpath/relation/orginv';

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
LOCATION '/relation/dstpath/relation/orghold/';

sudo -u hdfs hdfs dfs  -mkdir -p /relation/dstpath/relation/orghold/

sudo -u hdfs hdfs -cp /relation/dstpath/relation_data20170831/orghold /relation/dstpath/relation/orghold/

load data inpath '/relation/dstpath/relation_data20170831/orghold' into table org_hold_relation_gxs;
load data inpath '/relation/dstpath/relation_data20170831/ent' into table entbaseinfo_gxs;

/relation/dstpath/relation_data20170831/orghold
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

:START_ID(Org-ID)|holderrto|sharestype|holderamt|:END_ID(TenInv-ID)

--��֯�ṹ�عɹ�ϵ
CREATE EXTERNAL TABLE dev1_listedorg_gxs(
      orgid string,
      holderrto string,
      sharestype string,
      holderamt string,
      pripid string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/spark_test/listedorg/';

CREATE EXTERNAL TABLE dev1_listedorg_gxs(
      orgid string,
      holderrto string,
      sharestype string,
      holderamt string,
      pripid string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/tmp/spark_test/listedorg/';

b.entname, b.pripid, a.compcode

comp_info_tmp

CREATE EXTERNAL TABLE dev1_teninv_gxs(
              entname string,
              pripid string,
              compcode string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS  PARQUET
LOCATION '/relation/cachetable/comp_info_tmp';


