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
  var thReduce = 1f // pct of scattered data needed to start reducing
  var thComplete = 1f // pct of reduced data needed to complete current round
  var maxLag = 0 // number of rounds allowed for a worker to fall behind
  var round = -1 // current (unfinished) round of allreduce, can potentially be maxRound+1
  var maxRound = -1 // most updated timestamp received for StartAllreduce
  var maxScattered = -1 // most updated timestamp where scatter() has been called
  var completed = Set[Int]() // set of completed rounds

  // Data
  var dataSize = 0
  var data: Array[Float] = Array.empty // store input data
  var dataRange: Array[Int] = Array.empty
  var maxBlockSize = 0
  var myBlockSize = 0

  var scatterBlockBuf: DataBuffer = DataBuffer.empty // store scattered data received
  var reduceBlockBuf: DataBuffer = DataBuffer.empty // store reduced data received

  var maxChunkSize = 1024; // maximum msg size that is allowed on the wire

  var myNumChunks = 0
  var maxNumChunks = 0;

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
      maxChunkSize = init.maxChunkSize
      myNumChunks = math.ceil(1f * myBlockSize / maxChunkSize).toInt
      maxNumChunks = math.ceil(1f * maxBlockSize / maxChunkSize).toInt

      scatterBlockBuf = DataBuffer(
        dataSize = myBlockSize,
        peerSize = peers.size,
        maxLag = maxLag + 1,
        threshold = thReduce,
        maxChunkSize = maxChunkSize
      )

      reduceBlockBuf = DataBuffer(
        dataSize = maxBlockSize,
        peerSize = peers.size,
        maxLag=maxLag + 1,
        threshold = thComplete,
        maxChunkSize = maxChunkSize
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
        println(s"----Have not initialized!")
        self ! s
      } else {
        maxRound = math.max(maxRound, s.round)
        while (round < maxRound - maxLag) { // fall behind too much, catch up
          for (k <-0 until myNumChunks){
            val reducedData = reduce(0, k)
            broadcast(reducedData, k, round)
            //update(0)
          }
          complete(round)
        }
        while (maxScattered < maxRound) {
          fetch(maxScattered + 1)
          scatter()
          maxScattered += 1
        }
      }

    case s : ScatterBlock =>
      println(s"----receive scattered data from round ${s.round}: value = ${s.value.toList}, srcId = ${s.srcId}, destId = ${s.destId}, chunkId=${s.chunkId}, current round = $round")
      if (id == -1) {
        println(s"----Have not initialized!")
        self ! s
      } else {
        assert(s.destId == id)
        if (s.round < round || completed.contains(s.round)) {
          println(s"----Outdated scattered data")
        } else if (s.round <= maxRound) {
          val row = s.round - round
          scatterBlockBuf.store(s.value, row, s.srcId, s.chunkId)
          if(scatterBlockBuf.reachThreshold(row, s.chunkId)) {
            println(s"----receive ${scatterBlockBuf.count(row, s.chunkId)} scattered data (numPeers = ${peers.size}), chunkId =${s.chunkId} for round ${s.round}, start reducing")
            val reducedData = reduce(row, s.chunkId)
            broadcast(reducedData, s.chunkId, s.round)
          }
        } else {
          self ! StartAllreduce(s.round)
          self ! s
        }
      }

    case r : ReduceBlock =>
      println(s"----receive reduced data from round ${r.round}: value = ${r.value.toList}, srcId = ${r.srcId}, destId = ${r.destId}, chunkId=${r.chunkId}")
      if (id == -1) {
        println(s"----Have not initialized!")
        self ! r
      } else {
        assert(r.value.size <= maxChunkSize, s"Reduced block of size ${r.value.size} is larger than expected.. Max msg size is $maxChunkSize")
        assert(r.destId == id)
        if (r.round < round || completed.contains(r.round)) {
          println(s"----Outdated reduced data")
        } else if (r.round <= maxRound) {
          val row = r.round - round
          reduceBlockBuf.store(r.value, row, r.srcId, r.chunkId)
          if (reduceBlockBuf.reachRoundThreshold(row)) {
            println(s"----receive enough reduced data (numPeers = ${peers.size} for round ${r.round}, complete")
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
    Array.fill[Float](size)(0)
  }

  private def fetch(round : Int) = {
    println(s"fetch ${round}")
    data = Array.empty
    for (i <- 0 until dataSize) {
      data :+= i.toFloat + round
      println(s"----data[$i] = ${data(i)}")
    }
  }

  private def scatter() = {
    for ((idx, worker) <- peers) {
      val dataBlock = getDataBlock(idx)
     
      //Partition the dataBlock if it is too big
      for (i <- 0 until myNumChunks) {
        val chunkStart = math.min(i * maxChunkSize, dataBlock.length - 1);
        val chunkEnd = math.min((i + 1) * maxChunkSize - 1, dataBlock.length - 1);
        val chunk = new Array[Float](chunkEnd - chunkStart + 1);
        System.arraycopy(dataBlock, chunkStart, chunk, 0, chunk.length);
        //debug
        //println(s"----send msg ${chunk.toList} from ${id} to ${idx}, chunkId: ${i}")
        worker ! ScatterBlock(chunk, id, idx, i, maxScattered + 1);
      }
    }
  }

  private def initDataBlockRanges() = {
    val stepSize = dataSize / peers.size
    Array.range(0, dataSize, stepSize)
  }

  private def getDataBlock(idx: Int): Array[Float] = {
    val (start, end) = range(idx)
    val block = new Array[Float](end - start)
    System.arraycopy(data, start, block, 0, end - start)
    block
  }

  private def range(idx: Int): (Int, Int) = {
    if (idx >= peers.size - 1) 
      (dataRange(idx), data.size) 
    else 
      (dataRange(idx), dataRange(idx + 1))
  }

  private def broadcast(data: Array[Float], chunkId: Int, bcastRound: Int) = {
    println(s"----start broadcasting")
    for ((idx, worker) <- peers) {
        println(s"----Broadcast data:${data.toList}, src: ${id}, dest: ${idx}, chunkId: ${chunkId}, round: ${bcastRound}")
        worker ! ReduceBlock(data, id, idx, chunkId, bcastRound);
    }
  }

  private def reduce(row : Int, chunkId: Int) : Array[Float] = {
    println(s"----start reducing")
    val (unreduced, unreducedChunkSize) = scatterBlockBuf.get(row, chunkId)
    val reduced = initArray(unreducedChunkSize)
    for (i <- 0 until scatterBlockBuf.peerSize) {
      for (j <- 0 until unreducedChunkSize) {
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


  private def printBuffer(buf : Array[Array[Float]]) = {
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
    val worker = system.actorOf(Props[AllreduceWorker], name = "worker")

  }
}