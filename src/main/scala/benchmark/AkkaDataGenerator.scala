package benchmark

import java.util

import akka.actor.{Props, Actor, ActorSystem}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.util.Random

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
  private val actorNumber = conf.getInt("benchmarkMapDB.actorNumber")

  private val actors = {
    for (i <- 0 until actorNumber) yield actorSystem.actorOf(Props(new WorkerActor))
  }

  override def submitTask(key: Int, value: T): Unit = {
    actors(Random.nextInt(actorNumber)) ! Workload(key, value)
  }
}
