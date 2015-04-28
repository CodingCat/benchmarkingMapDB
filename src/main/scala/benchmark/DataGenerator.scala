package benchmark

import java.util

import scala.concurrent.ExecutionContext
import scala.util.Random

import com.typesafe.config.Config

class DataGenerator[T](conf: Config, concurrentMap: util.AbstractMap[Int, T], executor: ExecutionContext) {

  private val mode = conf.getString("benchmarkMapDB.mode")
  private val number = conf.getInt("benchmarkMapDB.workloadSize")
  private val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")

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

  private def submitTask(workload: Workload): Unit = {
    executor.execute(new Runnable {
      override def run() {
        try {
          concurrentMap.put(workload.key, workload.value.asInstanceOf[T])
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    })
  }

  def run(conf: Config) = mode match {
    case "vector" =>
      var i = 0
      while (i < number) {
        //generate a random vector
        try {
          val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
          submitTask(Workload(i, Vectors.sparse(vectorSize, newVector).asInstanceOf[SparseVector]))
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    case "int" =>
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
