package benchmark

import java.util

import scala.concurrent.ExecutionContext
import scala.util.Random

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.Config

class AkkaDataGenerator[T](conf: Config, concurrentMap: util.AbstractMap[Int, T], executor: ExecutionContext)
    extends DataGenerator[T](conf, concurrentMap, executor) {

  class WorkerIntActor extends Actor {
    override def receive: Receive = {
      case msg: Int =>
        concurrentMap.put(msg, msg.asInstanceOf[T])
    }
  }

  class WorkerVectorActor extends Actor {
    override def receive: Receive = {
      case msg: Workload =>
        concurrentMap.put(msg.key, msg.value.asInstanceOf[T])
    }
  }

  private val actorSystem = ActorSystem("testActorSystem", conf)
  private val actorNumber = conf.getInt("benchmarkMapDB.dataGenerator.akka.actorNumber")
  private val mode = conf.getString("benchmarkMapDB.mode")

  private val actors = {
    if (mode == "int") {
      for (i <- 0 until actorNumber) yield actorSystem.actorOf(Props(new WorkerIntActor))
    } else {
      for (i <- 0 until actorNumber) yield actorSystem.actorOf(Props(new WorkerVectorActor))
    }
  }

  private var roundRobinPointer = 0

  private def submitTask(msg: T): Unit = {
    actors(roundRobinPointer) ! msg
    if (roundRobinPointer < actorNumber - 1) {
      roundRobinPointer += 1
    } else {
      roundRobinPointer = 0
    }
  }

  override def run(conf: Config) = mode match {
    case "int" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      var i = 0
      while (i < number) {
        try {
          submitTask(i.asInstanceOf[T])
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    case "vector" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")
      var i = 0
      while (i < number) {
        try {
          //generate a random vector
          val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
          submitTask(Vectors.sparse(vectorSize, newVector).asInstanceOf[T])
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
  }
}
