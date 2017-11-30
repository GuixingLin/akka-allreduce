package sample.cluster.allreduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
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
  var data: Array[Double] = Array.empty // store input data
  var dataRange: Array[Int] = Array.empty
  var maxBlockSize = 0
  var myBlockSize = 0

  var scatterBlockBuf: DataBuffer = DataBuffer.empty // store scattered data received
  var reduceBlockBuf: DataBuffer = DataBuffer.empty // store reduced data received

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
      data = initArray(dataSize)
      dataRange = initDataBlockRanges()
      myBlockSize = blockSize(id)
      maxBlockSize = blockSize(0)

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
          val reducedData = reduce(0)
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
          scatterBlockBuf.store(s.value, row, s.srcId)
          if(scatterBlockBuf.reachThreshold(row)) {
            println(s"----receive ${scatterBlockBuf.count(row)} scattered data (numPeers = ${peers.size}) for round ${s.round}, start reducing")
            val reducedData = reduce(row)
            broadcast(reducedData, s.round)
          }
        } else {
          self ! StartAllreduce(s.round)
          self ! s
        }
      }

    case r : ReduceBlock =>
      assert(r.value.size <= maxBlockSize, s"Reduced block of size ${r.value.size} is larger than expected.. Max block size is $maxBlockSize")
      println(s"----receive reduced data from round ${r.round}: value = ${r.value.toList}, srcId = ${r.srcId}, destId = ${r.destId}")
      if (id == -1) {
        println(s"----I am not initialized yet!!!")
        self ! r
      } else {
        assert(r.destId == id)
        if (r.round < round || completed.contains(r.round)) {
          println(s"----Outdated reduced data")
        } else if (r.round <= maxRound) {
          val row = r.round - round
          reduceBlockBuf.store(r.value, row, r.srcId)
          if (reduceBlockBuf.reachThreshold(row)) {
            println(s"----receive ${reduceBlockBuf.count(row)} reduced data (numPeers = ${peers.size}) for round ${r.round}, complete")
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

  private def initArray(size: Int) = {
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
      val dataBlock = getDataBlock(idx)
      println(s"----send msg ${dataBlock.toList} from ${id} to ${idx}")
      worker ! ScatterBlock(dataBlock, id, idx, maxScattered + 1)
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

  private def broadcast(data: Array[Double], bcastRound: Int) = {
    println(s"----start broadcasting")
    for ((idx, worker) <- peers) {
      worker ! ReduceBlock(data, id, idx, bcastRound)
    }
  }

  private def reduce(row : Int) : Array[Double] = {
    println(s"----start reducing")
    val unreduced = scatterBlockBuf.get(row)
    val reduced = initArray(myBlockSize)

    for (i <- 0 until scatterBlockBuf.peerSize) {
      for (j <- 0 until myBlockSize) {
        reduced(j) += unreduced(i)(j)
      }
    }
    return reduced
  }

  private def update(row : Int) = {
    println(s"----update round ${round + row}")
  }

  private def complete(completedRound : Int) = {
    println(s"----complete allreduce round ${completedRound}\n")
    data = Array.empty
    master.orNull ! CompleteAllreduce(id, completedRound)
    completed = completed + completedRound
    if (round == completedRound) {
      do {
        round += 1
        scatterBlockBuf.up()
        reduceBlockBuf.up()
      } while (completed.contains(round))
    }
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