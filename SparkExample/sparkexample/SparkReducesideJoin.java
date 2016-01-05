package org.sparkexample;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext.*;
import org.apache.spark.api.*;

object SparkMapsideJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SparkMapsideJoin")
    conf.setMaster("local[3]")
    conf.set("spark.shuffle.manager", "sort");
    val sc = new SparkContext(conf)

    //val table1 = sc.textFile(args(1))
    //val table2 = sc.textFile(args(2))

    val table1 = sc.parallelize(List("k1,v11", "k2,v12", "k3,v13"))
    val table2 = sc.parallelize(List("k1,v21", "k4,v24", "k3,v23"))
    // table1 is smaller, so broadcast it as a map<String, String>
    val pairs = table1.map { x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }.collectAsMap
    val broadCastMap = sc.broadcast(pairs) //save table1 as map, and broadcast it

    // table2 join table1 in map side
    val result = table2.map { x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }.mapPartitions({ iter =>
      val m = broadCastMap.value
      for {
        (key, value) <- iter
        if (m.contains(key))
      } yield (key, (value, m.get(key).getOrElse("")))
    })

    val output = "d:/wordcount-" + System.currentTimeMillis() ;
    result.saveAsTextFile(output) //save result to local file or HDFS
  }
}
