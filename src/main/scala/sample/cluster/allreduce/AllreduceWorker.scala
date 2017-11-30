package sample.cluster.allreduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import sample.cluster.allreduce.buffer.DataBuffer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class AllreduceWorker extends Actor {

  var id = -1 // node id
  var master: Option[ActorRef] = None
  var peers = Map[Int, ActorRef]() // workers in the same row/col, including self
  var thReduce = 1.0 // pct of scattered data needed to start reducing
  var thComplete = 1.0 // pct of reduced data needed to complete current round
  var maxLag = 0 // number of rounds allowed for a worker to fall behind
  var round = -1 // current (unfinished) round of allreduce, can potentially be maxRound+1
  var maxRound = -1 // most updated timestamp received for StartAllreduce
  var maxScattered = -1 // most updated timestamp where scatter() has been called
  var completed = Set[Int]() // set of completed rounds

  // Data
  var dataSize = 0
  var blockSize = 0
  var data: Array[Double] = Array.empty // store input data
  var dataRange: Array[Int] = Array.empty

  var scatterBuf: Array[Array[Double]] = Array.empty // store scattered data received
  var reduceBuf: Array[Array[Double]] = Array.empty // store reduced data received
  var reducedData: Double = 0 // result of reducing scatterBuf

  var scatterBlockBuf: DataBuffer = DataBuffer.empty
  var reduceBlockBuf: DataBuffer = DataBuffer.empty

  def receive = {

    case init: InitWorkers =>
      id = init.destId
      master = Some(init.master)
      peers = init.workers
      thReduce = init.thReduce
      thComplete = init.thComplete
      maxLag = init.maxLag
      round = 0 // clear round info to start over
      maxRound = -1
      maxScattered = -1
      completed = Set[Int]()

      dataSize = init.dataSize
      data = initializeData(dataSize)
      dataRange = initDataBlockRanges()

      scatterBuf = new Array[Array[Double]](maxLag + 1)
      reduceBuf = new Array[Array[Double]](maxLag + 1)
      reducedData = 0

      val myBlockSize = blockSize(id)
      val maxBlockSize = blockSize(0)

      for (row <- 0 to maxLag) {
//        scatterBuf(row) = new Array[Double](peers.size + 1) // extra space for tracking number of elems
        reduceBuf(row) = new Array[Double](peers.size + 1)
      }
      scatterBlockBuf = DataBuffer(
        dataSize=myBlockSize,
        peerSize = peers.size,
        maxLag = maxLag + 1,
        threshold = thReduce
      )

      reduceBlockBuf = DataBuffer(
        dataSize=maxBlockSize,
        peerSize = peers.size,
        maxLag=maxLag + 1,
        threshold = thComplete
      )

      println(s"----id = ${id}")
      for (i <- 0 until peers.size) {
        println(s"----peers[${i}] = ${peers(i)}")
      }
      println(s"----number of peers = ${peers.size}")
      println(s"----thReduce = ${thReduce}, thComplete = ${thComplete}, maxLag = ${maxLag}")
      println(s"----size of buffer: ${scatterBlockBuf.maxLag} x ${scatterBlockBuf.peerSize} x ${scatterBlockBuf.dataSize}")

    case s : StartAllreduce =>
      println(s"----start allreduce round ${s.round}")
      if (id == -1) {
        println(s"----I am not initialized yet!!!")
        self ! s
      } else {
        maxRound = math.max(maxRound, s.round)
        while (round < maxRound - maxLag) { // fall behind too much, catch up
          reducedData = reduce(0)
          broadcast(reducedData, round)
          update(0)
          complete(round)
        }
        while (maxScattered < maxRound) {
          fetch(maxScattered + 1)
          scatter()
          maxScattered += 1
        }
      }

    case s : ScatterBlock =>
      println(s"----receive scattered data from round ${s.round}: value = ${s.value}, srcId = ${s.srcId}, destId = ${s.destId}, current round = $round")
      if (id == -1) {
        println(s"----I am not initialized yet!!!")
        self ! s
      } else {
        assert(s.destId == id)
        if (s.round < round || completed.contains(s.round)) {
          println(s"----Outdated scattered data")
        } else if (s.round <= maxRound) {
          val row = s.round - round
          storeScatterBlockData(s.value, s.srcId, row)
//          if (scatterBuf(row)(peers.size) == peers.size * thReduce) {
          if(scatterBlockBuf.reachThreshold(row)) {
            println(s"----receive ${scatterBlockBuf.count(row)} scattered data (numPeers = ${peers.size}) for round ${s.round}, start reducing")
            reducedData = reduce(row)
            broadcast(reducedData, s.round)
          }
        } else {
          self ! StartAllreduce(s.round)
          self ! s
        }
      }

    case r : ReduceBlock =>
      println(s"----receive reduced data from round ${r.round}: value = ${r.value}, srcId = ${r.srcId}, destId = ${r.destId}")
      if (id == -1) {
        println(s"----I am not initialized yet!!!")
        self ! r
      } else {
        assert(r.destId == id)
        if (r.round < round || completed.contains(r.round)) {
          println(s"----Outdated reduced data")
        } else if (r.round <= maxRound) {
          val row = r.round - round
          storeReducedData(r.value, r.srcId, row)
          if (reduceBuf(row)(peers.size) == peers.size * thComplete) {
            println(s"----receive ${reduceBuf(row)(peers.size)} reduced data (numPeers = ${peers.size}) for round ${r.round}, complete")
            update(row)
            complete(r.round)
          }
        } else {
          self ! StartAllreduce(r.round)
          self ! r
        }
      }


    case Terminated(a) =>
      for ((idx, worker) <- peers) {
        if(worker == a) {
          peers -= idx
        }
      }
  }


  private def blockSize(id: Int) = {
    val (start, end) = range(id)
    end - start
  }

  private def initializeData(size: Int) = {
    Array.fill[Double](size)(0)
  }

  private def fetch(round : Int) = {
    println(s"fetch ${round}")
    data = Array.empty
    for (i <- 0 until dataSize) {
      data :+= i.toDouble + round
      println(s"----data[$i] = ${data(i)}")
    }
  }

  private def scatter() = {
    for ((idx, worker) <- peers) {
      println(s"----send msg ${data(idx)} from ${id} to ${idx}")
      worker ! ScatterBlock(getDataBlock(idx), id, idx, maxScattered + 1)
      //      worker ! Scatter(data(idx), id, idx, maxScattered + 1)
    }
  }

  private def initDataBlockRanges() = {
    val stepSize = dataSize / peers.size
    Array.range(0, dataSize, stepSize)
  }

  private def getDataBlock(idx: Int): Array[Double] = {
    val (start, end) = range(idx)
    val block = new Array[Double](end - start)
    System.arraycopy(data, start, block, 0, end - start)
    block
  }

  private def range(idx: Int): (Int, Int) = {
    if (idx >= peers.size - 1) (dataRange(idx), data.size) else (dataRange(idx), dataRange(idx + 1))
  }

  private def broadcast(data: Double, bcastRound: Int) = {
    println(s"----start broadcasting")
    for ((idx, worker) <- peers) {
      worker ! ReduceBlock(Array(data), id, idx, bcastRound)
    }
  }

  private def storeScatteredData(data : Double, srcId : Int, row : Int) = {
    scatterBuf(row)(srcId) = data
    scatterBuf(row)(peers.size) += 1
  }

  private def storeScatterBlockData(data: Array[Double], srcId: Int, row: Int) = {
    scatterBlockBuf.store(data, row, srcId)
  }

  private def storeReducedData(data: Array[Double], srcId : Int, row : Int) = {
    reduceBuf(row)(srcId) = data(0)
    reduceBuf(row)(peers.size) += 1
  }

  private def reduce(row : Int) : Double = {
    println(s"----start reducing")
    reducedData = 0

    for (col <- 0 until peers.size) {
      //TODO: here assume each array double is only one!!
      reducedData += scatterBlockBuf.get(row)(col).sum
    }
    return reducedData
  }

  private def update(row : Int) = {
    println(s"----update round ${round + row}")
  }

  private def complete(completedRound : Int) = {
    println(s"----complete allreduce round ${completedRound}\n")
    data = Array.empty
    master.orNull ! CompleteAllreduce(id, completedRound)
    completed+(completedRound)
    if (round == completedRound) {
      do {
        round += 1
//        bufferUp(scatterBuf)
        scatterBlockBuf.up()
        bufferUp(reduceBuf)
      } while (completed.contains(round))
    }
  }

  private def bufferUp(buf : Array[Array[Double]]) = {
    for (i <- 1 until buf.size) {
      buf(i-1) = buf(i)
    }
    buf(buf.size - 1) = new Array[Double](peers.size + 1)
  }

  private def printBuffer(buf : Array[Array[Double]]) = {
    for (r <- 0 until buf.size) {
      print("----")
      for (c <- 0 until buf(r).size) {
        print(s"${buf(r)(c)} ")
      }
      println("")
    }
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