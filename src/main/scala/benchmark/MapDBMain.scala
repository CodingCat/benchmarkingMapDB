package benchmark

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadPoolExecutor}

import com.typesafe.config.ConfigFactory
import org.mapdb._

import scala.concurrent.ExecutionContext
import scala.util.Random

object MapDBMain {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.parseFile(new File(args(0)))
    val dataGenerator = new DataGenerator
    val workload = dataGenerator.run(conf)
    val executor = conf.getString("benchmarkMapDB.executor") match {
      case "threadpool" =>
        ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
      case "forkjoin" =>
        ExecutionContext.global
    }
    val hashMap = conf.getString("benchmarkMapDB.collection") match {
      case "MapDB" =>
        DBMaker.newMemoryDB().make().createHashMap("HTreeMap").
          keySerializer(Serializer.INTEGER).
          valueSerializer(Serializer.INTEGER).
          make[Int, Int]()
      case _ =>
        new ConcurrentHashMap[Int, Int]()
    }
    println("start")
    val startMoment = System.nanoTime()
    for (w <- workload) {
      executor.execute(new Runnable {
        override def run(): Unit = {
          hashMap.put(Random.nextInt(), w.asInstanceOf[Int])
        }
      })
    }
    val endMoment = System.nanoTime()
    println("elapsedTime: " + (endMoment - startMoment) + " nanoseconds")
  }
}
