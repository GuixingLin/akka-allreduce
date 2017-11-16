package sample.cluster.allreduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class AllreduceWorker extends Actor {

  var id = -1 // node id
  var master : Option[ActorRef] = None
  var peers = Map[Int, ActorRef]()  // workers in the same row/col, including self
  var thReduce = 1.0    // pct of scattered data needed to start reducing 
  var thComplete = 1.0  // pct of reduced data needed to complete current round
  var maxLag = 0    // number of rounds allowed for a worker to fall behind
  var data : Array[Double] = Array.empty  // input buffer
  var scatterBuf : Array[Double] = Array.empty  // buffer to store scattered data received
  var reduceBuf : Array[Double] = Array.empty   // buffer to store reduced data received
  var aggregatedData  : Double = 0  // obtained by aggregating scatterBuf
  var round = -1 // current round of allreduce

  def receive = {

    case init : InitWorkers =>
      peers = init.workers
      master = Some(init.master)
      id = init.destId
      thReduce = init.thReduce
      thComplete = init.thComplete
      maxLag = init.maxLag
      round = -1 // clear round info to start over
      println(s"----id = ${id}")
      for (i <- 0 until peers.size) {
        println(s"----peers[${i}] = ${peers(i)}")
      }
      println(s"----number of peers = ${peers.size}")

    case start : StartAllreduce =>
      println(s"----start allreduce round ${start.round}")
      if (start.round > round) {
        round = start.round
        initData(round)
        scatter()
      }

    case s : Scatter =>
      println(s"----receive scattered data from round ${s.round}: value = ${s.value}, srcId = ${s.srcId}, destId = ${s.destId}")
      assert(s.destId == id || id == -1)
      storeScatteredData(s.value, s.srcId) //? necessary?
      if (scatterBuf.length >= peers.size * thReduce && id != -1) {
        println(s"----receive ${scatterBuf.length} scattered data (numPeers = ${peers.size}) for round ${round}, start reducing")
        aggregatedData = aggregate()
        broadcast(aggregatedData)
      }

    case r : Reduce =>
      println(s"----receive reduced data from round ${r.round}: value = ${r.value}, srcId = ${r.srcId}, destId = ${r.destId}")
      assert(r.destId == id || id == -1)
      storeReducedData(r.value, r.srcId)
      if (reduceBuf.length >= peers.size * thComplete && id != -1) {
        println(s"----receive ${reduceBuf.length} reduced data (numPeers = ${peers.size}) for round ${round}, complete")
        update()
        complete(round)
      }

    case Terminated(a) =>
      for ((idx, worker) <- peers) {
        if(worker == a) {
          peers -= idx
        }
      }
  }

  private def initData(round : Int) = {
    for (i <- 0 until peers.size) {
      data :+= i.toDouble + round
      println(s"----data[$i] = ${data(i)}")
    }
  }

  private def scatter() = {
    for ((idx, worker) <- peers) {
      println(s"----send msg ${data(idx)} from ${id} to ${idx}")
      worker ! Scatter(data(idx), id, idx, round)
    }
  }

  private def broadcast(data : Double) = {
    println(s"----start broadcasting")
    for ((idx, worker) <- peers) {
      worker ! Reduce(data, id, idx, round)
    }
  }

  private def storeScatteredData(data : Double, srcId : Int) = {
    scatterBuf :+= data
  }

  private def storeReducedData(data: Double, srcId : Int) = {
    reduceBuf :+= data
  }

  private def aggregate() : Double = {
    println(s"----start aggregating")
    aggregatedData = 0
    for (v <- scatterBuf) {
      aggregatedData += v
    }
    return aggregatedData
  }

  private def update() = {
    println("----in update")
  }

  private def complete(round : Int) = {
    println(s"----complete allreduce round ${round}\n")
    scatterBuf = Array.empty
    reduceBuf = Array.empty 
    data = Array.empty
    master.orNull ! CompleteAllreduce(id, round)
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