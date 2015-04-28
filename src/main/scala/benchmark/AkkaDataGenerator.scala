package benchmark

import java.util

import scala.concurrent.ExecutionContext
import scala.util.Random

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.Config

class AkkaDataGenerator[T](conf: Config, concurrentMap: util.AbstractMap[Int, T], executor: ExecutionContext)
    extends DataGenerator[T](concurrentMap, executor) {

  case class Workload(key: Int, v: T)

  class WorkerActor extends Actor {
    override def receive: Receive = {
      case msg @ Workload(_, _) =>
        concurrentMap.put(msg.key, msg.v)
    }
  }

  private val actorSystem = ActorSystem("testActorSystem", conf)
  private val actorNumber = conf.getInt("benchmarkMapDB.dataGenerator.akka.actorNumber")

  private val actors = {
    for (i <- 0 until actorNumber) yield actorSystem.actorOf(Props(new WorkerActor))
  }

  private var roundRobinPointer = 0

  private def submitTask(msg: Workload): Unit = {
    actors(roundRobinPointer) ! msg
    if (roundRobinPointer < actorNumber - 1) {
      roundRobinPointer += 1
    } else {
      roundRobinPointer = 0
    }
  }

  override def run(conf: Config) = conf.getString("benchmarkMapDB.mode") match {
    case "int" =>
      val number = conf.getInt("benchmarkMapDB.workloadSize")
      var i = 0
      while (i < number) {
        try {
          submitTask(Workload(i, Random.nextInt().asInstanceOf[T]))
          i += 1
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
  }
}
