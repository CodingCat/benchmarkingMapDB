package benchmark

import java.util.concurrent.ConcurrentHashMap

import scala.util.Random

import com.typesafe.config.Config
import org.mapdb.{DB, DBMaker, Serializer}

class DataGenerator[T](conf: Config) {

  private val number = conf.getInt("benchmarkMapDB.workloadSize")
  private val vectorSize = conf.getInt("benchmarkMapDB.vectorSize")
  private val workloadType = conf.getString("benchmarkMapDB.workloadType")

  protected val concurrentMap = initializeCollection()

  private val workloadRunner = new LoadExecutor[T](conf, concurrentMap)

  private def initDB(): DB = {
    var dbMaker = DBMaker.
      newMemoryDirectDB().
      transactionDisable()
    val asyncDelay = conf.getInt("benchmarkMapDB.MapDB.asyncDelay")
    if (asyncDelay > 0) {
      val asyncQueueSize = conf.getInt("benchmarkMapDB.MapDB.asyncQueueSize")
      dbMaker = dbMaker.asyncWriteEnable().asyncWriteFlushDelay(asyncDelay).asyncWriteQueueSize(asyncQueueSize)
    }
    dbMaker.make()
  }

  private def initializeMapDBHashMap() = {
    val hashMapMaker = initDB().
      createHashMap("HTreeMap").
      counterEnable().
      keySerializer(Serializer.INTEGER)
    if (workloadType == "int") {
      hashMapMaker.valueSerializer(Serializer.INTEGER).make[Int, T]()
    } else {
      hashMapMaker.make[Int, T]()
    }
  }

  private def initializeMapDBTreeMap()= {
    val nodeSize = conf.getInt("benchmarkMapDB.treeMap.nodeSize")
    val treeMapMaker = initDB().
      createTreeMap("BTreeMap").
      counterEnable().
      keySerializer(Serializer.INTEGER).nodeSize(nodeSize)
    if (workloadType == "int") {
      treeMapMaker.valueSerializer(Serializer.INTEGER).make[Int, T]()
    } else {
      treeMapMaker.make[Int, T]()
    }
  }

  private def initOnHeapCollection() = new ConcurrentHashMap[Int, T]()

  private def initializeCollection() = {
    conf.getString("benchmarkMapDB.collection") match {
      case "MapDBHashMap" =>
        initializeMapDBHashMap()
      case "MapDBTreeMap" =>
        initializeMapDBTreeMap()
      case "ConcurrentHashMap" =>
        initOnHeapCollection()
    }
  }


  private def startMonitorThread(): Unit = {
    println("start")
    val startMoment = System.nanoTime()
    //start monitor thread
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val num = conf.getInt("benchmarkMapDB.workloadSize")
        while (concurrentMap.size() < num) {
          Thread.sleep(2000)
          println("processed " + concurrentMap.size())
        }
        val endMoment = System.nanoTime()
        println("elapsedTime: " + (endMoment - startMoment) + " nanoseconds")
      }
    })
    t.start()
  }

  def run(conf: Config) = {
    var i = 0
    while (i < number) {
      //generate a random vector
      try {
        if (workloadType == "vector") {
          val newVector = for (j <- 0 until vectorSize) yield (j, Random.nextDouble())
          workloadRunner.submitLoad(newVector.asInstanceOf[T])
        } else {
          workloadRunner.submitLoad(i.asInstanceOf[T])
        }
        i += 1
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  startMonitorThread()
}
