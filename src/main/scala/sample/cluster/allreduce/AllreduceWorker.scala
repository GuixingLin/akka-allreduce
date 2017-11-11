package sample.cluster.allreduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class AllreduceWorker extends Actor {

  var myId = -1
  var myGroup: collection.mutable.Map[Int,ActorRef] = collection.mutable.Map[Int, ActorRef]()
  var groupSize = -1
  var numScattered = 0
  var numGathered = 0
  var scatteredData : Array[Double] = Array.empty
  var scatteredDataRecv : Array[Double] = Array.empty
  var gatheredData : Array[Double] = Array.empty
  var aggregatedData : Double = 0

  def receive = {

    case nb : Neighbors=>
      myGroup = nb.workers
      myId = nb.destId
      groupSize = myGroup.size
      scatteredDataRecv = Array[Double](groupSize)
      gatheredData = Array[Double](groupSize)
      println(s"----myId = ${myId}")

    case start : StartAllreduce =>
      initScatteredData()
      println(s"scattered data = ${scatteredData(0)} ${scatteredData(1)}")
      scatter()

    case scattered : Scatter =>
      assert(scattered.destId == myId)
      println(s"receive scattered data = $scattered.value")
      storeScatteredData(scattered.value, scattered.srcId) //? necessary?

      if (numScattered == groupSize) {
        aggregatedData = aggregate()
        broadcast(aggregatedData)
      }

    case gathered : Gather =>
      assert(gathered.destId == myId)
      storeGatheredData(gathered.value, gathered.srcId)
      if (numGathered == groupSize) {
        update()
        complete()
      }

    case Terminated(a) =>
      for ((idx, worker) <- myGroup) {
        if(worker == a) {
          myGroup -= idx
        }
      }
  }

  private def initScatteredData() = {
    println(s"scatteredData.length = ${scatteredData.length}")
    for (i <- 0 until scatteredData.length) {
      scatteredData :+= i.toDouble
      println(s"scatteredData[$i] = ${scatteredData(i)}")
    }
  }

  private def scatter() = {
    for ((idx, worker) <- myGroup) {
      worker ! Scatter(scatteredData(idx), myId, idx)
    }
  }

  private def broadcast(data : Double) = {
    for ((idx, worker) <- myGroup) {
      worker ! Gather(data, myId, idx)
    }
  }

  private def storeScatteredData(data : Double, srcId : Int) = {
    scatteredDataRecv(srcId) = data
  }

  private def storeGatheredData(data: Double, srcId : Int) = {
    gatheredData(srcId) = data
  }

  private def aggregate() : Double = {
    aggregatedData = 0
    for (v <- scatteredDataRecv) {
      aggregatedData += v
    }
    return aggregatedData
  }

  private def update() = {
    println("----in update")
  }

  private def complete() = {
    println("----in complete")
  }

}

object AllreduceWorker {
  def main(args: Array[String]): Unit = {
    val port = if (args.isEmpty) "2553" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val worker = system.actorOf(Props[AllreduceWorker], name = "worker") //?

    //?
    // system.scheduler.schedule(2.seconds, 2.seconds) {
    //   implicit val timeout = Timeout(5 seconds)
    //   worker ? GreetGroups() onSuccess {
    //     case s => println(s)
    //   }
    // }
  }

  // def startUp() = {
  //   main(Array())
  // }
}