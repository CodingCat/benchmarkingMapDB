package benchmark

import java.util

import scala.concurrent.ExecutionContext
import scala.util.Random

import com.typesafe.config.Config

class DataGenerator[T](concurrentMap: util.AbstractMap[Int, T], executor: ExecutionContext) {

  private def submitTask(key: Int, value: T): Unit = {
    executor.execute(new Runnable {
      override def run() {
        try {
          concurrentMap.put(key, Random.nextInt().asInstanceOf[T])
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    })
  }

  def run(conf: Config) = conf.getString("benchmarkMapDB.mode") match {
    case "vector" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")
      var i = 0
      while (i < number) {
        //generate a random vector
        try {
          val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
          submitTask(i, Vectors.sparse(vectorSize, newVector).asInstanceOf[T])
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    case "int" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      var i = 0
      while (i < number) {
        try {
          submitTask(i, Random.nextInt().asInstanceOf[T])
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
  }
}
