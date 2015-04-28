package benchmark

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadPoolExecutor}

import com.typesafe.config.ConfigFactory
import org.mapdb._

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Random

object MapDBMain {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.parseFile(new File(args(0)))
    val executor = conf.getString("benchmarkMapDB.executor") match {
      case "threadpool" =>
        ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
      case "forkjoin" =>
        new ForkJoinPool
        ExecutionContext.global
    }
    val hashMap = conf.getString("benchmarkMapDB.collection") match {
      case "MapDB" =>
        DBMaker.
          newMemoryDirectDB().
          transactionDisable().
          make().
          createHashMap("HTreeMap").
          counterEnable().
          keySerializer(Serializer.INTEGER).
          valueSerializer(Serializer.INTEGER).
          make[Int, Int]()
      case _ =>
        new ConcurrentHashMap[Int, Int]()
    }
    val dataGenerator = new DataGenerator(hashMap, executor)
    println("start")
    val startMoment = System.nanoTime()
    //start monitor thread
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val num = conf.getInt("benchmarkMapDB.workloadSize")
        while (hashMap.size() < num) {
          Thread.sleep(100)
          println("processed " + hashMap.size())
        }
        val endMoment = System.nanoTime()
        println("elapsedTime: " + (endMoment - startMoment) + " nanoseconds")
      }
    })
    t.start()
    dataGenerator.run(conf, hashMap)
  }
}
