package com.chinadaas.association.main



import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.hashing.MurmurHash3
import org.apache.log4j.{Level, Logger}


object AMLApp {

  case class Node(zsid: String, name: String, pripid: String, conprop: String)


  case class Person(nodenum: String, pripid: String, usedname: String, jobid: String)

  case class Aml235(level: Int, root: String, path: String, iscyclic: Int, none: Int, score: Double)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("spark AML App").getOrCreate();

    val sc = spark.sparkContext;

  /*  val empData = Array(
      ("EMP001", null.asInstanceOf[String],0)
       ,("EMP010", null.asInstanceOf[String],0)
      , ("EMP005", "EMP010",4)
      , ("EMP002", "EMP001",2)
//      , ("EMP003", "EMP001",3)
//      , ("EMP004", "EMP001",3)
      , ("EMP005", "EMP002",4)
//      , ("EMP005", "EMP003",4)
//      , ("EMP005", "EMP004",4)
      , ("EMP006", "EMP005",4)
//      , ("EMP005", "Sam", "Cap", "Lead",null.asInstanceOf[String],0)
//      , ("EMP006", "Ron", "Hubb", "Sr.Dev", "EMP005",6)
//      , ("EMP007", "Cathy", "Watson", "Dev", "EMP005",7)
//      , ("EMP008", "Samantha", "Lion", "Dev", "EMP007",8)
//      , ("EMP008", "Jimmy", "Copper", "Dev", "EMP006",9)
//      , ("EMP010", "Shon", "Taylor", "Intern", "EMP008",10)
//      , ("EMP02", "Shon", "Taylor", "Intern", "EMP01",11)
//      , ("EMP01", "Shon", "Taylor", "Intern", "EMP02",11)
    )


    val site = "Runoob" :: ("Google" :: ("Baidu" :: Nil))

    val site = "ri" :: ("sss" :: ("baud":: Nil))


    val empDF = sc.parallelize(empData, 3).toDF("emp_id","mgr_id","score").cache()
    empDF.createOrReplaceTempView("risk")
//     primary key , root, path - dataframe to graphx for vertices
    val empVertexDF = spark.sql("select  emp_id from risk")
    val vertexDF = spark.sql("select distinct emp_id from risk")

//     parent to child - dataframe to graphx for edges
    val empEdgeDF = empDF.selectExpr("emp_id","mgr_id","score").filter("mgr_id is not null")
    val edgeDF = empDF.selectExpr("mgr_id","emp_id","score").filter("mgr_id is not null")

*/



    val person =  spark.read.textFile("/inceptorsql1/user/hive/warehouse/default.db/hive/s_en_usedname_full/data_date=20171211").rdd.
      map{s =>
        var bzxr = s.split("\\u0001", -1)
        Person(bzxr(0),bzxr(1),bzxr(2),bzxr(3))
      };


    spark.createDataFrame(person).createOrReplaceTempView("s_en_usedname")


    val orgnode = spark.read.textFile("/relation/dstpath/relation_data20171212/orginvmerge").rdd.map { s =>
      var bzxr = s.split("\\|", -1)
      var zsid = bzxr(0).substring(bzxr(0).lastIndexOf("-") + 1)
      var conprop = ""
      if (bzxr(1) == null || "" == bzxr(1))
        conprop = bzxr(9)
      else
        conprop = bzxr(1)
      Node(zsid, zsid, bzxr(2), conprop);
    }
    spark.createDataFrame(orgnode).createOrReplaceTempView("orgnode")

    spark.sql("select md5(name) as zsid ,pripid,conprop from orgnode where length(conprop)<4 and conprop<>''").createOrReplaceTempView("orgrelation")

    val person = spark.read.textFile("/relation/dstpath/relation_data20171212/personinvmerge").rdd.map { s =>
      var bzxr = s.split("\\|", -1);
      var conprop = ""
      if (bzxr(1) == null || "" == bzxr(1)) {
        conprop = bzxr(9)
      } else {
        conprop = bzxr(1)
      }
      Node(bzxr(0), bzxr(0), bzxr(2), conprop)
    }

    spark.createDataFrame(person).createOrReplaceTempView("personnode")

    spark.sql("select zsid,pripid,conprop from personnode where length(conprop)<4 and length(conprop)<>1 ").createOrReplaceTempView("personrelation")

    val entnode = spark.read.textFile("/relation/dstpath/relation_data20171212/entinvmerge").rdd.map { s =>
      var bzxr = s.split("\\|", -1);
      var conprop = ""
      if (bzxr(1) == null || "" == bzxr(1)) {
        conprop = bzxr(9)
      } else {
        conprop = bzxr(1)
      }
      Node(bzxr(0), bzxr(0), bzxr(2), conprop)
    }

    spark.createDataFrame(entnode).createOrReplaceTempView("entnode")
    //企业叶子节点
    spark.sql("select distinct a.zsid,a.pripid,a.conprop from entnode a where length(a.conprop)<4 and a.conprop<>'' and zsid not in ('320100000000061401448','320100000000079850252','320100000000095736051') ").createOrReplaceTempView("relation")

   /* spark.sql("select  zsid ,pripid,conprop from orgrelation " +
      "union " +
      "select zsid,pripid,conprop from personrelation " +
      "union " +
      "select  a.zsid,a.pripid,a.conprop from entrelation  a ").cache().createOrReplaceTempView("relation")
*/

    val empEdgeDF = spark.sql("select * from relation ").cache()

    spark.sql("select * from (select zsid from relation union select pripid as zsid  from relation) ").createOrReplaceTempView("relation2")

    val empVertexDF = spark.sql("select * from relation2 group by zsid").cache()



    val empHirearchyExtDF1hi = calcTopLevelHierarcy(empVertexDF, empEdgeDF).map { case (pk, (level, root, path, isCyclic, none, score,hui)) =>
        (level, root, path, isCyclic, none, score)
      }.map(x => Aml235(x._1, x._2, x._3, x._4, x._5, x._6))


    spark.createDataFrame(empHirearchyExtDF1hi).write.parquet("/tmp/spark_test/inv_fanxiqian")

    spark.stop()

  }


  // primary key , root, path - dataframe to graphx for vertices

  // parent to child - dataframe to graphx for edges




  def calcTopLevelHierarcy(vertexDF: DataFrame, edgeDF: DataFrame): RDD[(Any, (Int, String, String, Int, Int, Double,Set[VertexId]))] = {

    // create the vertex RDD
    // primary key, root, path
    val verticesRDD = vertexDF
      .rdd
      .map { x => (x.get(0)) }
      .map { x => (MurmurHash3.stringHash(x.toString).toLong, x.toString) }

    // create the edge RDD
    // top down relationship
    val EdgesRDD = edgeDF.rdd.map { x => (x.get(0), x.get(1), x.get(2)) }
      .map { x => Edge(MurmurHash3.stringHash(x._1.toString).toLong, MurmurHash3.stringHash(x._2.toString).toLong, x._3.toString.toDouble) }

    // create graph
    val graph = Graph(verticesRDD, EdgesRDD).cache()

    val pathSeperator = """/"""

    // initialize id,level,root,path,iscyclic, isleaf,score
    val initialMsg = (0L, 0, "", List("dummy"), 0, 1, 0.0,"",Map[VertexId,Double]())


    // add more dummy attributes to the vertices - id, level, root, path, isCyclic, existing value of current vertex to build path, isleaf, pk
    val initialGraph = graph.mapVertices((id, v) => (
      if (id==56565205)
        (id, 0, v.toString, List(v.asInstanceOf[String]), 0, v.toString, 1, 0.asInstanceOf[Any], 0.0,Map(id -> 0.0))
      else
        (id, 0, v.toString, List(v.asInstanceOf[String]), 0, v.toString, 1, 0.asInstanceOf[Any], 0.0,Map[VertexId,Double]())
    ))


    val hrchyRDD = initialGraph.pregel(initialMsg,
      Int.MaxValue,
      EdgeDirection.Out)(
      setMsg,
      sendMsg,
      mergeMsg)

    // build the path from the list
    val hrchyOutRDD = hrchyRDD.vertices.map { case (id, v) => (v._8, (v._2, v._3, pathSeperator + v._4.reverse.mkString(pathSeperator), v._5, v._7, v._9,v._10.keySet)) }

    hrchyOutRDD
  }


  //mutate the value of the vertices
  def setMsg(vertexId: VertexId, value: (Long, Int, String, List[String], Int, String, Int, Any, Double,Map[VertexId,Double]),
             message: (Long, Int, String, List[String], Int, Int, Double,String,Map[VertexId,Double])):
  (Long, Int, String, List[String], Int, String, Int, Any, Double,Map[VertexId,Double]) = {

    if (message._2 < 1) { //superstep 0 - initialize
      // id, level, root, path, isCyclic, existing value of current vertex to build path, isleaf, pk
      println("message._2"+(value._1, value._2 + 1, value._3, value._4, value._5, value._6, value._7, value._8, message._7,message._9))
      (value._1, value._2 + 1, value._3, value._4, value._5, value._6, value._7, value._8, message._7,message._9)
    } else if (message._5 == 1) { // set isCyclic
      println("message._5 "+(value._1, value._2, value._3, value._4, message._5, value._6, value._7, value._8, value._9),value._10)
      (value._1, value._2, value._3, value._4, message._5, value._6, value._7, value._8, value._9,message._9)
    } else if (message._6 == 0) { // set isleaf
      println("message._6 "+(value._1, value._2, value._3, value._4, value._5, value._6, message._6, value._8, value._9,value._10))
      (value._1, value._2, value._3, value._4, value._5, value._6, message._6, value._8, value._9,message._9)
    } else { // set new values
      println("message else"+(message._1, value._2 + 1, message._3, value._6 :: message._4, value._5, value._6, value._7, value._8, message._7,value._10))
      (message._1, value._2 + 1, message._3, value._6 :: message._4, value._5, value._6, value._7, value._8, message._7,message._9)
    }
  }


  // send the value to vertices
  def sendMsg(triplet: EdgeTriplet[(Long, Int, String, List[String], Int, String, Int, Any, Double,Map[VertexId,Double]), Double]):
  Iterator[(VertexId, (Long, Int, String, List[String], Int, Int, Double,String,Map[VertexId,Double]))] = {
    val sourceVertex = triplet.srcAttr
    val destinationVertex = triplet.dstAttr
    // check for icyclic
    if (sourceVertex._1 == triplet.dstId || sourceVertex._1 == destinationVertex._1)
      if (destinationVertex._5 == 0) { //set iscyclic
        println("destinationVertex._5"+(triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 1, sourceVertex._7, sourceVertex._9 + triplet.attr)))
        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 1, sourceVertex._7,
          sourceVertex._9 + triplet.attr,destinationVertex._6,Map(triplet.dstId -> triplet.attr))))
      } else {
        println("empty"+"yes")
        Iterator.empty
      }
    else {
      if (sourceVertex._7 == 1) //is NOT leaf
      {
        println("sourceVertex._7 == 1"+(triplet.srcId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4,sourceVertex._5 , 0, sourceVertex._9,destinationVertex._6)))
        Iterator((triplet.srcId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4,
          sourceVertex._5 , 0, sourceVertex._9,destinationVertex._6,Map(triplet.dstId -> triplet.attr))))
      }
      else { // set new values
        println("else" + (triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, sourceVertex._5 , 1, sourceVertex._9 + triplet.attr,destinationVertex._6)))
        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3,
          sourceVertex._4, sourceVertex._5 , 1, sourceVertex._9 + triplet.attr,destinationVertex._6,Map(triplet.dstId -> triplet.attr))))
      }

    }
  }

  // receive the values from all connected vertices
  def mergeMsg(msg1: (Long, Int, String, List[String], Int, Int, Double,String,Map[VertexId,Double]),
               msg2: (Long, Int, String, List[String], Int, Int, Double,String,Map[VertexId,Double])):
  (Long, Int, String, List[String], Int, Int, Double,String,Map[VertexId,Double]) = {
    // dummy logic not applicable to the data in this usecase
    println("msg1"+msg1)
    println("msg2"+msg2)
    //合并
    if(msg1._8==msg2._8){
      val pathSeperator="->"
      val mes1value = pathSeperator + msg1._4.reverse.mkString(pathSeperator)
      val mes2value = pathSeperator + msg2._4.reverse.mkString(pathSeperator)
      val merge = List(mes1value +"|"+ mes2value)
      (msg1._1,msg1._2,msg1._3,merge,msg1._5,msg1._6,msg1._7+msg2._7,msg1._8,msg1._9 ++ msg2._9)

    }else{
      msg2
    }

  }

  //Int,String,String,Int,Int,Double

}
