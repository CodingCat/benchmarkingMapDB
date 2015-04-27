package benchmark

import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.util.Random

class DataGenerator {

  def run(conf: Config) = conf.getString("benchmarkMapDB.mode") match {
    case "vector" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")
      val vectorBuffer = new ListBuffer[SparseVector]
      for (i <- 0 until number) {
        //generate a random vector
        val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
        vectorBuffer += Vectors.sparse(vectorSize, newVector).asInstanceOf[SparseVector]
      }
      vectorBuffer.toList
    case "int" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val intBuffer = new ListBuffer[Int]
      for (i <- 0 until number) {
        intBuffer += i
      }
      intBuffer.toList
  }
}
