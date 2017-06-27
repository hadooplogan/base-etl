--1找出不 占比 <>1 的企业 probleDataTmp01
create table probleDataTmp01 as
select *
  from (select a.pripid, b.regno, b.entname,b.REGCAP, a.conam / b.REGCAP as bili
          from (select pripid, sum(SUBCONAM) as conam
                  from e_inv_investment_hdfs_ext_20170529
                 group by pripid) a
          join (select pripid, REGCAP, regno, entname
                 from enterprisebaseinfocollect_hdfs_ext_20170529
                where entstatus = '1') b
            on a.pripid = b.pripid
         where b.REGCAP <> '0'
           and b.REGCAP <> '') f
 where f.bili > 1.05
    or f.bili < 0.95;

 --2、找出企业对应的年报信息  probleDataTmp02
 create table probleDataTmp02 as
  select b.ancheid, a.pripid
    from probleDataTmp01 a
    join C_GS_AN_BASEINFO b
      on a.regno = b.regno
     and a.entname = b.entname;

   create table CGSANPAIDUPCAPITAL as
   select b.ancheid, b.regno, b.entname
     from (select pripid, max(ancheyear) ancheyear
             from C_GS_AN_BASEINFO
            group by pripid) a
     join C_GS_AN_BASEINFO b
       on a.pripid = b.pripid
      and a.ancheyear = b.ancheyear;
 --3找出企业对应的股东 probleDataTmp03
  create table probleDataTmp03 as
  select a.pripid, b.inv,b.liacconam
    from (select b.ancheid, a.pripid
            from probleDataTmp01 a
            join C_GS_AN_BASEINFO b
              on a.regno = b.regno
             and a.entname = b.entname) a
    join C_GS_AN_PAIDUPCAPITAL b
      on a.ancheid = b.ancheid;

--4年报股东信息 probleDataTmp04
create table probleDataTmp04 as
 select pripid, collect_set(inv) as inv
   from (select pripid,inv, row_number() over(partition by pripid order by inv) rk
           from probleDataTmp03 a)
  group by pripid;
--5 股东表信息 probleDataTmp05
 create table probleDataTmp05 as
 select pripid, collect_set(inv) as inv
   from (select *, row_number() over(partition by pripid order by inv) rk
           from (select a.pripid, b.inv
                   from probleDataTmp01 a
                   join e_inv_investment_hdfs_ext_20170529 b
                     on a.pripid = b.pripid) a)
  group by pripid;

--6 判断股东数量和名字是否相等 1 相等 probleDataTmp06
 create table probleDataTmp06 as
select pripid
  from (select pripid, collectsame(a.inv, b.inv) as sety
          from probleDataTmp03 a
          join probleDataTmp04 b
            on a.pidpid = b.pripid)
 where sety = 1;
 --7 取出年报表中占比为1的pripid create table probleDataTmp06 as
 create table probleDataTmp07 as
 select a.pripid
   from (select a.pripid, b.regno
           from probleDataTmp01 a
           join probleDataTmp05 b
             on a.pripid = b.pripid) a
   join (select a.pripid, sum(liacconam) as liacconam
           from probleDataTmp03 a
          group by pripid) b
     on a.pripid = b.pripid
  where 0.95 < liacconam / a.regno < 1.05;
 --8 合并结果集
select a.s_ext_nodenum,
       a.pripid,
       a.s_ext_sequence,
       a.invid,
       a.inv,
       a.invtype,
       a.certype,
       a.cerno,
       a.blictype,
       a.blicno,
       a.country,
       a.currency,
       a.subconam,
       a.acconam,
       a.subconamusd,
       a.acconamusd,
       a.conprop,
       a.conform,
       a.condate,
       a.baldelper,
       c.liacconam,
       a.exeaffsign,
       a.s_ext_timestamp,
       a.s_ext_batch,
       a.s_ext_validflag,
       a.linkman,
       a.cerno_old,
       a.subconam_new,
       a.conprop_new,
       a.status,
       a.record_stat,
       a.record_desc,
       a.match,
       a.zspid
  from e_inv_investment_hdfs_ext_20170529
  join probleDataTmp06
    on a.pripid = b.pripid
  join probleDataTmp03
    on a.pripid = c.pripid
   and a.inv = c.inv
union
  select a.s_ext_nodenum,
         a.pripid,
         a.s_ext_sequence,
         a.invid,
         a.inv,
         a.invtype,
         a.certype,
         a.cerno,
         a.blictype,
         a.blicno,
         a.country,
         a.currency,
         a.subconam,
         a.acconam,
         a.subconamusd,
         a.acconamusd,
         a.conprop,
         a.conform,
         a.condate,
         a.baldelper,
         a.conam,
         a.exeaffsign,
         a.s_ext_timestamp,
         a.s_ext_batch,
         a.s_ext_validflag,
         a.linkman,
         a.cerno_old,
         a.subconam_new,
         a.conprop_new,
         a.status,
         a.record_stat,
         a.record_desc,
         a.match,
         a.zspid
    from e_inv_investment_hdfs_ext_20170529
    join probleDataTmp06
      on pripid = prpid
   where a.pripid is null;