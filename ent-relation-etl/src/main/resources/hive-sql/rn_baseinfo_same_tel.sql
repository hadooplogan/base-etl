-- 同电话名单
create table rn_baseinfo_same_tel_20170508 as 
select tel
  from (select tel, count(distinct pripid) tel_count
          from (select *
                  from enterprisebaseinfocollect_hdfs_ext_20170508
                 where tel regexp '^(13|15|18|17)[0-9]{9}$'
                    or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'
                    or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\-|\(\\|\*|\-|-|\ ]?)?([2-9][0-9]{6,7})(\-[0-9]{1,4})?'
                    or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?'
                    or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'
                    or tel regexp
                 '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?[-|(|)}）|\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?' where entstatus = '1')
           and tel is not null
           and tel <> ''
         group by tel
        having count(distinct pripid) > 1 and count(distinct pripid) < 5)
 where length(tel) > 5;
create table rn_baseinfo_same_tel_rela_20170508 as 
select me.pripid,
       ai.tel,
       concat_ws('#', me.pripid, '2') as spid,
       ai.tel as epid
  from (select tel
          from (select tel, count(distinct pripid) tel_count
                  from (select *
                          from enterprisebaseinfocollect_hdfs_ext_20170508
                         where tel regexp '^(13|15|18|17)[0-9]{9}$'
                            or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'
                            or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\-|\(\\|\*|\-|-|\ ]?)?([2-9][0-9]{6,7})(\-[0-9]{1,4})?'
                            or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?'
                            or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'
                            or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?[-|(|)}）|\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?'
                           and entstatus = '1')
                 where tel is not null
                   and tel <> ''
                 group by tel
                having count(distinct pripid) > 1 and count(distinct pripid) <= 5)
         where length(tel) > 5) ai
 inner join (select distinct tel, pripid
               from enterprisebaseinfocollect_hdfs_ext_20170508
              where tel regexp '^(13|15|18|17)[0-9]{9}$'
                 or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'
                 or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\-|\(\\|\*|\-|-|\ ]?)?([2-9][0-9]{6,7})(\-[0-9]{1,4})?'
                 or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?'
                 or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'
                 or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?[-|(|)}）|\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?'
                and entstatus = '1') me
    on ai.tel = me.tel
 where ai.tel is not null
   and ai.tel <> '';
