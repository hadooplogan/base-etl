基础数据处理
==========
date: 2017-06-30 11:50:43

一、基础结构
-------
* maven构建
* base-etl 项目结构
* common-etl公共包说明

**使用maven**

使用git将代码下载到本地仓库，通过maven下载相关依赖包，依赖包下载后使用，maven install 打包，打包成功后会在targe目录下生成jar包

**base-etl 项目结构**
````
   base-etl
    |---common-etl（公共）
    |---ent-relation-etl（各自业务数据etl）
    |--ent-next-etl
````
**common-etl公共包说明**

封装一些公共的、基本功能包，例如工具类：文件操作、MD5操作、时间操作等spark sql 对字段处理的UDF,其他业务处理工程需要依赖common包
使用的时候可以直接调用，也可以维护common

二、基础功能
-----
#### 1、log日志功能（待完善）

* Spark日志功能
* 日志配置
* 日志使用说明
* 日志查看

#### 2、发布部署（待完善）
* 发布部署流程

目前是打包所有依赖jar，进行spark-submit，后期希望配置classpath环境变量，甚至自动打包部署

#### 3、Spark监控页面
* Spark on standalone监控

  UI页面：[http://192.168.100.101:4040/stages/](http://192.168.100.101:4040/stages/)
  
* Spark on yarn监控

  UI页面：[http://192.168.100.101:8088/cluster/apps/RUNNING](http://192.168.100.101:8088/cluster/apps/RUNNING)

### 4、Spark  job 提交

* Spark on yarn 方式
````
/usr/lib/spark/spark-1.5.2-bin-hadoop2.4/bin/spark-submit 
--executor-memory 10G 
--num-executors 8 
--executor-cores 10  
--class com.chinadaas.association.etl.main.EntRelationApp 
--master yarn ent-relation-etl-1.0-SNAPSHOT1.jar

````

#### 5、参数说明
* 常用参数

--executor-memory 每个executor内存大小

--num-executors executors的数量

--executor-cores 每个executors的cpu核数


详见官方文档
[http://spark.apache.org/docs/2.1.1/configuration.html](http://spark.apache.org/docs/2.1.1/configuration.html)