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
  var maxLag = 0        // number of rounds allowed for a worker to fall behind
  var round = -1        // current round of allreduce
  var maxRound = -1 // most updated timestamp received for StartAllreduce
  var data : Array[Double] = Array.empty            // store input data
  var scatterBuf : Array[Array[Double]] = Array.empty  // store scattered data received
  var reduceBuf : Array[Array[Double]] = Array.empty   // store reduced data received
  var aggregatedData : Double = 0  // result of aggregating scatterBuf

  def receive = {

    case init : InitWorkers =>
      peers = init.workers
      master = Some(init.master)
      id = init.destId
      thReduce = init.thReduce
      thComplete = init.thComplete
      maxLag = init.maxLag
      round = -1    // clear round info to start over
      maxRound = -1
      scatterBuf = new Array[Array[Double]](maxLag + 1)
      reduceBuf = new Array[Array[Double]](maxLag + 1)
      for (row <- 0 to maxLag) {
        scatterBuf(row) = new Array[Double](peers.size + 1) // extra space for tracking number of elems
        reduceBuf(row) = new Array[Double](peers.size + 1)
      }
      println(s"----id = ${id}")
      for (i <- 0 until peers.size) {
        println(s"----peers[${i}] = ${peers(i)}")
      }
      println(s"----number of peers = ${peers.size}")
      println(s"----thReduce = ${thReduce}, thComplete = ${thComplete}, maxLag = ${maxLag}")
      println(s"----size of buffer: ${scatterBuf.size} x ${scatterBuf(0).size}")

    case start : StartAllreduce =>
      println(s"----start allreduce round ${start.round}")
      if (start.round > round) {
        round = start.round
        bufferUp(scatterBuf)
        bufferUp(reduceBuf)
        initData(round)
        scatter()
      }

    case s : Scatter =>
      println(s"----receive scattered data from round ${s.round}: value = ${s.value}, srcId = ${s.srcId}, destId = ${s.destId}")
      assert(s.destId == id || id == -1)
      val row = s.round - round //??? what if round == -1?
      storeScatteredData(s.value, s.srcId, row)
      if (scatterBuf(row)(peers.size) >= peers.size * thReduce && id != -1) {
        println(s"----receive ${scatterBuf(row)(peers.size)} scattered data (numPeers = ${peers.size}) for round ${round}, start reducing")
        aggregatedData = aggregate()
        broadcast(aggregatedData)
      }

    case r : Reduce =>
      println(s"----receive reduced data from round ${r.round}: value = ${r.value}, srcId = ${r.srcId}, destId = ${r.destId}")
      assert(r.destId == id || id == -1)
      val row = r.round - round
      storeReducedData(r.value, r.srcId, row)
      if (reduceBuf(row)(peers.size) >= peers.size * thComplete && id != -1) {
        println(s"----receive ${reduceBuf(row)(peers.size)} reduced data (numPeers = ${peers.size}) for round ${round}, complete")
        update()
        complete(round)
        if (round < maxRound) {
          self ! StartAllreduce(round + 1)
        }
      }

    case Terminated(a) =>
      for ((idx, worker) <- peers) {
        if(worker == a) {
          peers -= idx
        }
      }
  }

  private def bufferUp(buf : Array[Array[Double]]) = {
    for (i <- 1 until buf.size) {
      buf(i-1) = buf(i)
    }
    buf(buf.size - 1) = new Array[Double](peers.size + 1)
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

  private def storeScatteredData(data : Double, srcId : Int, row : Int) = {
    scatterBuf(row)(srcId) = data
    scatterBuf(row)(peers.size) += 1
  }

  private def storeReducedData(data: Double, srcId : Int, row : Int) = {
    reduceBuf(row)(srcId) = data
    reduceBuf(row)(peers.size) += 1
  }

  private def aggregate() : Double = {
    println(s"----start aggregating")
    aggregatedData = 0
    for (col <- 0 until peers.size) {
      aggregatedData += scatterBuf(0)(col)
    }
    return aggregatedData
  }

  private def update() = {
    println("----in update")
  }

  private def complete(round : Int) = {
    println(s"----complete allreduce round ${round}\n")
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