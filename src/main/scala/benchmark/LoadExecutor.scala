package benchmark

import java.util.concurrent.{ConcurrentMap, Executors}

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Random

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.Config

/**
 * a wrapper class for running the load
 * it wraps the threadpool, forkjoin pool or ActorSystem
 */
class LoadExecutor[T](conf: Config, concurrentMap: ConcurrentMap[Int, T]) {

  private case class Workload(key: Int, value: T)

  private class WorkerActor extends Actor {
    override def receive: Receive = {
      case msg @ Workload(_, _) =>
        concurrentMap.put(msg.key, msg.value)
    }
  }

  private val executorName = conf.getString("benchmarkMapDB.executor.name")
  private val parallelism = conf.getInt("benchmarkMapDB.executor.parallelism")
  private var newKey = 0
  private lazy val actorSystem = ActorSystem("LoadRunner", conf)
  private lazy val actors = {
    val actorNumber = parallelism
    for (i <- 0 until actorNumber) yield actorSystem.actorOf(Props(new WorkerActor))
  }

  private lazy val executor = {
    executorName match {
      case "threadpool" =>
        if (parallelism > 0) {
          ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))
        } else {
          ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
        }
      case "forkjoin" =>
        if (parallelism > 0) {
          ExecutionContext.fromExecutor(new ForkJoinPool(parallelism))
        } else {
          ExecutionContext.global
        }
    }
  }

  def submitLoad(load: T) = executorName match {
    case "akka" =>
      val currentKey = newKey
      actors(Random.nextInt() % parallelism) ! Workload(currentKey, load)
      newKey += 1
    case _ =>
      val currentKey = newKey
      executor.execute(new Runnable {
        override def run(): Unit = {
          concurrentMap.put(currentKey, load)
        }
      })
      newKey += 1
  }
}
