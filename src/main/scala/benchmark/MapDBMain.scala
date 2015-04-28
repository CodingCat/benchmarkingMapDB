package benchmark

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors}

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.existentials

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
    val generatorName = conf.getString("benchmarkMapDB.dataGenerator.name")
    var dataGenerator: DataGenerator[_] = null

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
          val r = hashMapMaker.valueSerializer(Serializer.INTEGER).make[Int, Int]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[Int](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[Int](conf, r, executor)
          }
          r
        } else {
          val r = hashMapMaker.make[Int, SparseVector]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[SparseVector](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[SparseVector](conf, r, executor)
          }
          r
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
          val r = treeMapMaker.valueSerializer(Serializer.INTEGER).make[Int, Int]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[Int](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[Int](conf, r, executor)
          }
          r
        } else {
          val r = treeMapMaker.make[Int, SparseVector]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[SparseVector](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[SparseVector](conf, r, executor)
          }
          r
        }
      case _ =>
        if (mode == "int") {
          val r = new ConcurrentHashMap[Int, Int]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[Int](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[Int](conf, r, executor)
          }
          r
        } else {
          val r = new ConcurrentHashMap[Int, SparseVector]()
          if (generatorName == "default") {
            dataGenerator = new DataGenerator[SparseVector](conf, r, executor)
          } else {
            dataGenerator = new AkkaDataGenerator[SparseVector](conf, r, executor)
          }
          r
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
