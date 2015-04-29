package benchmark

import java.io.File

import scala.language.existentials

import com.typesafe.config.ConfigFactory

object MapDBMain {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.parseFile(new File(args(0)))
    val dataGenerator: DataGenerator[_] = {
      val workloadType = conf.getString("benchmarkMapDB.workloadType")
      workloadType match {
        case "int" =>
          new DataGenerator[Int](conf)
        case "vector" =>
          new DataGenerator[SparseVector](conf)
      }
    }
    dataGenerator.run(conf)
  }
}
