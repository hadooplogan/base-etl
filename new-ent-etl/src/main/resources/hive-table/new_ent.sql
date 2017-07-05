--1 baseinfo
create external table  NEW_ENT_BASEINFO
(
PRIPID string,
S_EXT_NODENUM string,
ENTNAME string,
ORIREGNO string,
REGNO string,
ENTTYPE string,
PPRIPID string,
PENTNAME string,
PREGNO string,
HYPOTAXIS string,
INDUSTRYPHY string,
INDUSTRYCO string,
ABUITEM string,
CBUITEM string,
OPFROM string,
OPTO string,
POSTALCODE string,
TEL string,
EMAIL string,
LOCALADM string,
CREDLEVEL string,
ASSDATE string,
ESDATE string,
APPRDATE string,
REGORG string,
ENTCAT string,
ENTSTATUS string,
REGCAP double,
OPSCOPE string,
OPFORM string,
OPSCOANDFORM string,
PTBUSSCOPE string,
DOMDISTRICT string,
DOM string,
ECOTECDEVZONE string,
DOMPRORIGHT string,
OPLOCDISTRICT string,
OPLOC string,
RECCAP double,
INSFORM string,
PARNUM int,
PARFORM string,
EXENUM int,
EMPNUM int,
SCONFORM string,
FORCAPINDCODE string,
MIDPREINDCODE string,
PROTYPE string,
CONGRO double,
CONGROCUR string,
CONGROUSD double,
REGCAPUSD double,
REGCAPCUR string,
REGCAPRMB double,
FORREGCAPCUR string,
FORREGCAPUSD double,
FORRECCAPUSD double,
WORCAP double,
CHAMECDATE string,
OPRACTTYPE string,
FORENTNAME string,
DEPINCHA string,
COUNTRY string,
ITEMOFOPORCPRO string,
CONOFCONTRPRO string,
FORDOM string,
FORREGECAP double,
FOROPSCOPE string,
S_EXT_ENTPROPERTY string,
S_EXT_TIMESTAMP String,
S_EXT_BATCH string,
S_EXT_SEQUENCE String,
S_EXT_VALIDFLAG string,
S_EXT_INDUSCAT string,
S_EXT_ENTTYPE string,
MANACATE string,
LIMPARNUM int,
FOREIGNBODYTYPE string,
PERSON_ID string,
NAME string,
CERTYPE string,
CERNO string,
ANCHEYEAR string,
CANDATE string,
REVDATE string,
ENTNAME_OLD string,
CREDIT_CODE string,
JOBID string,
IS_NEW int,
COUNTRYDISPLAY string,
STATUSDISPLAY string,
TYPEDISPLAY string,
REGORGDISPLAY string,
REGCAPCURDISPLAY string,
ENTID string,
HANDLE_TYPE int,
TAX_CODE string,
LICID string,
ZSPID string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES("field.delim"='\t',"serialization.encoding"='GBK')

--hive中执行load

--load data  inpath '/tmp/new_ent/ENTERPRISEBASEINFOCOLLECT.txt' overwrite into table NEW_ENT_BASEINFO;
--load data  inpath '/tmp/new_ent/E_INV_INVESTMENT.txt' overwrite into table NEW_ENT_INVINFO;
--load data  inpath '/tmp/new_ent/E_PRI_PERSON.txt' overwrite into table NEW_ENT_PERSONINFO;
 --2、
  create  external table NEW_ENT_INVINFO
(
S_EXT_NODENUM string ,
PRIPID string,
INVID string,
INV string,
INVTYPE string,
CERTYPE string,
CERNO string,
BLICTYPE string,
BLICNO string,
COUNTRY string,
CURRENCY string,
SUBCONAM double,
ACCONAM double,
SUBCONAMUSD double,
ACCONAMUSD double,
CONPROP double,
CONFORM string,
CONDATE string,
BALDELPER string,
CONAM double,
EXEAFFSIGN string,
S_EXT_TIMESTAMP string,
S_EXT_BATCH string,
S_EXT_SEQUENCE string  ,
S_EXT_VALIDFLAG string,
LINKMAN string,
JOBID string,
ENT_ID string,
TYPEDISPLAY string,
HANDLE_TYPE int,
ZSPID string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES("field.delim"='\t',"serialization.encoding"='GBK')


 --3
 create external table NEW_ENT_PERSONINFO
(
S_EXT_NODENUM string,
PRIPID string,
PERSON_ID string,
NAME string,
CERTYPE string,
CERNO string,
SEX string,
NATDATE string,
DOM string,
POSTALCODE string,
TEL string,
LITDEG string,
NATION string,
POLSTAND string,
OCCST string,
OFFSIGN string,
ACCDSIDE string,
LEREPSIGN string,
CHARACTER string,
COUNTRY string,
ARRCHDATE string,
CERLSSDATE string,
CERVALPER string,
CHIOFTHEDELSIGN string,
S_EXT_TIMESTAMP string,
S_EXT_BATCH string,
NOTORG string,
NOTDOCNO string,
S_EXT_SEQUENCE string,
S_EXT_VALIDFLAG string,
POSITION string,
OFFHFROM string,
OFFHTO string,
POSBRFORM string,
APPOUNIT string,
JOBID string,
ENTID string,
TYPEDISPLAY string,
DATA_FLAG string,
HANDLE_TYPE string,
ZSPID string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES("field.delim"='\t',"serialization.encoding"='GBK')


--4 临时表
create external table  NEW_ENT_BASEINFO_MID
(
PRIPID string,
S_EXT_NODENUM string,
ENTNAME string,
ORIREGNO string,
REGNO string,
ENTTYPE string,
PPRIPID string,
PENTNAME string,
PREGNO string,
HYPOTAXIS string,
INDUSTRYPHY string,
INDUSTRYCO string,
ABUITEM string,
CBUITEM string,
OPFROM string,
OPTO string,
POSTALCODE string,
TEL string,
EMAIL string,
LOCALADM string,
CREDLEVEL string,
ASSDATE string,
ESDATE string,
APPRDATE string,
REGORG string,
ENTCAT string,
ENTSTATUS string,
REGCAP double,
OPSCOPE string,
OPFORM string,
OPSCOANDFORM string,
PTBUSSCOPE string,
DOMDISTRICT string,
DOM string,
ECOTECDEVZONE string,
DOMPRORIGHT string,
OPLOCDISTRICT string,
OPLOC string,
RECCAP double,
INSFORM string,
PARNUM int,
PARFORM string,
EXENUM int,
EMPNUM int,
SCONFORM string,
FORCAPINDCODE string,
MIDPREINDCODE string,
PROTYPE string,
CONGRO double,
CONGROCUR string,
CONGROUSD double,
REGCAPUSD double,
REGCAPCUR string,
REGCAPRMB double,
FORREGCAPCUR string,
FORREGCAPUSD double,
FORRECCAPUSD double,
WORCAP double,
CHAMECDATE string,
OPRACTTYPE string,
FORENTNAME string,
DEPINCHA string,
COUNTRY string,
ITEMOFOPORCPRO string,
CONOFCONTRPRO string,
FORDOM string,
FORREGECAP double,
FOROPSCOPE string,
S_EXT_ENTPROPERTY string,
S_EXT_TIMESTAMP String,
S_EXT_BATCH string,
S_EXT_SEQUENCE String,
S_EXT_VALIDFLAG string,
S_EXT_INDUSCAT string,
S_EXT_ENTTYPE string,
MANACATE string,
LIMPARNUM int,
FOREIGNBODYTYPE string,
PERSON_ID string,
NAME string,
CERTYPE string,
CERNO string,
ANCHEYEAR string,
CANDATE string,
REVDATE string,
ENTNAME_OLD string,
CREDIT_CODE string,
JOBID string,
IS_NEW int,
COUNTRYDISPLAY string,
STATUSDISPLAY string,
TYPEDISPLAY string,
REGORGDISPLAY string,
REGCAPCURDISPLAY string,
ENTID string,
HANDLE_TYPE int,
TAX_CODE string,
LICID string,
ZSPID string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES("field.delim"='\t',"serialization.encoding"='GBK')

insert overwrite table NEW_ENT_BASEINFO_MID
select * from NEW_ENT_BASEINFO
where esdate between '20160701' and '20170630'
and pripid is not  null  and entstatus = 1


select count(1) from NEW_ENT_BASEINFO
where esdate between '20160630' and '20170629'
and  name is not null   and pripid is not  null  and entstatus = 1
and ( regno is not null or credit_code is not null )

--select count(*) from NEW_ENT_BASEINFO_20170629  --5987182
--select count(*) from NEW_ENT_INVINFO_20170629--10995283
--select count(1) from  NEW_ENT_PERSONINFO_20170629  --15114966
--select count(*) from NEW_ENT_BASEINFO_TEL_20170619 --1855622
--select count(*) from NEW_ENT_BASEINFO_DOM_20170619 --1757368


drop table NEW_ENT_BASEINFO;
ALTER TABLE NEW_ENT_BASEINFO_20170629 RENAME TO NEW_ENT_BASEINFO;

drop table NEW_ENT_INVINFO;
ALTER TABLE NEW_ENT_INVINFO_20170629 RENAME TO NEW_ENT_INVINFO;

drop table NEW_ENT_PERSONINFO;
ALTER TABLE NEW_ENT_PERSONINFO_20170629 RENAME TO NEW_ENT_PERSONINFO;

drop table NEW_ENT_BASEINFO_TEL;
ALTER TABLE NEW_ENT_BASEINFO_TEL_20170619 RENAME TO NEW_ENT_BASEINFO_TEL;

drop table NEW_ENT_BASEINFO_DOM;
ALTER TABLE NEW_ENT_BASEINFO_DOM_20170619 RENAME TO NEW_ENT_BASEINFO_DOM;

select count(*) from NEW_ENT_BASEINFO  --5987182
select count(*) from NEW_ENT_INVINFO--10995283
select count(1) from  NEW_ENT_PERSONINFO   --15114966



select count(*) from NEW_ENT_BASEINFO_TEL --1855622
select count(*) from NEW_ENT_BASEINFO_DOM --1757368







