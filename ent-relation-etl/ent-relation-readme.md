## 关联洞察业务说明

#### 主程序入口
* EntInfo2EsApp
* EntRelationApp
* FlushConpropApp

**EntInfo2EsApp**

1. 业务说明

2. 清洗规则

3. 处理流程

**EntRelationApp**

1. 业务说明

2. 清洗规则

3. 处理流程


**FlushConpropApp**

1. 业务说明

2. 清洗规则

3. 处理流程

##依赖关系
 1.最先执行CommonETL将常用的Hive表转化为parquet格式,以后直接使用loadparquet方式提高程序性能。
 
 2.其次执行FlushConpropApp，将占比数据刷入为parquet。
 
 3.执行EntRelation，关联洞察业务。
 
 4.执行EntInfo2EsApp 将最新数据刷入Es5






