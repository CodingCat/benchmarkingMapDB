package benchmark

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors}

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

import com.typesafe.config.ConfigFactory
import org.mapdb._

object MapDBMain {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.parseFile(new File(args(0)))
    val parallelism = conf.getInt("benchmarkMapDB.executioncontext.parallelism")
    val executor = conf.getString("benchmarkMapDB.executioncontext.executor") match {
      case "threadpool" =>
        if (parallelism > 0) {
          ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))
        } else {
          ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
        }
      case "forkjoin" =>
        if (parallelism > 0) {
          ExecutionContext.fromExecutor(new ForkJoinPool(parallelism))
        } else {
          ExecutionContext.global
        }
    }

    val mode = conf.getString("benchmarkMapDB.mode")

    val hashMap = conf.getString("benchmarkMapDB.collection") match {
      case "MapDBHashMap" =>
        val hashMapMaker = DBMaker.
          newMemoryDirectDB().
          transactionDisable().
          make().
          createHashMap("HTreeMap").
          counterEnable().
          keySerializer(Serializer.INTEGER)
        if (mode == "int") {
          hashMapMaker.valueSerializer(Serializer.INTEGER).make[Int, Int]()
        } else {
          hashMapMaker.make[Int, SparseVector]()
        }
      case "MapDBTreeMap" =>
        val treeMapMaker = DBMaker.
          newMemoryDirectDB().
          transactionDisable().
          make().
          createTreeMap("BTreeMap").
          counterEnable().
          keySerializer(Serializer.INTEGER)
        if (mode == "int") {
          treeMapMaker.valueSerializer(Serializer.INTEGER).
            make[Int, Int]()
        } else {
          treeMapMaker.make[Int, SparseVector]()
        }
      case _ =>
        if (mode == "int") {
          new ConcurrentHashMap[Int, Int]()
        } else {
          new ConcurrentHashMap[Int, SparseVector]()
        }
    }
    val dataGenerator = {
      val generatorName = conf.getString("benchmarkMapDB.dataGenerator.name")
      if (generatorName == "default") {
        new DataGenerator(conf, hashMap, executor)
      } else if (generatorName == "akka") {
        new AkkaDataGenerator(conf, hashMap, executor)
      } else {
        throw new Exception("invalid dataGenerator Name:" + generatorName)
      }
    }
    println("start")
    val startMoment = System.nanoTime()
    //start monitor thread
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val num = conf.getInt("benchmarkMapDB.workloadSize")
        while (hashMap.size() < num) {
          Thread.sleep(2000)
          println("processed " + hashMap.size())
        }
        val endMoment = System.nanoTime()
        println("elapsedTime: " + (endMoment - startMoment) + " nanoseconds")
      }
    })
    t.start()
    dataGenerator.run(conf)
  }
}
