package benchmark

import java.util
import java.util.concurrent.ForkJoinPool

import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.util.Random

class DataGenerator(executor: ExecutionContext) {

  def run[T](conf: Config, concurrentMap: util.AbstractMap[Int, T]) = conf.getString("benchmarkMapDB.mode") match {
    case "vector" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")
      for (i <- 0 until number) {
        //generate a random vector
        val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
        executor.execute(new Runnable {
          override def run() {
            concurrentMap.put(i, Vectors.sparse(vectorSize, newVector).asInstanceOf[T])
          }
        })
      }
    case "int" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val a = new ForkJoinPool()
      for (i <- 0 until number) {
        try {
          executor.execute(new Runnable {
            override def run() {
              try {
                concurrentMap.put(i, Random.nextInt().asInstanceOf[T])
              } catch {
                case e: Exception =>
                  e.printStackTrace()
              }
            }
          })
        } catch {
          case e: Exception =>
            executor.execute(new Runnable {
              override def run() {
                try {
                  concurrentMap.put(i, Random.nextInt().asInstanceOf[T])
                } catch {
                  case e: Exception =>
                    e.printStackTrace()
                }
              }
            })
        }
      }
  }
}
