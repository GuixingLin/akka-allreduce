import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import sample.cluster.allreduce._

import scala.util.Random

class AllReduceSpec extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  type DataSink = AllReduceOutput => Unit
  type DataSource = AllReduceInputRequest => AllReduceInput

  //basic setup
  val source: DataSource = createBasicDataSource(8)
  val sink: DataSink = r => {
    println(s"Data output at #${r.iteration}: ${r.data.toList}")
  }

  def createBasicDataSource(size: Int): DataSource = {
    createCustomDataSource(size) {
      (idx: Int, iter: Int) => idx + iter.toFloat
    }
  }

  def createCustomDataSource(size: Int)(arrIdxAndIterToData: (Int, Int) => Float): DataSource = {
    req => {
      val floats = Array.range(0, size).map(arrIdxAndIterToData(_, req.iteration))
      println(s"Data source at #${req.iteration}: ${floats.toList}")
      AllReduceInput(floats)
    }
  }

  def assertiveDataSink(expected: List[List[Float]], iterations: List[Int]): DataSink = {
    r => {
      val pos = iterations.indexOf(r.iteration)
      pos should be >= 0
      r.data.toList shouldBe expected(pos)
    }
  }

  "Flushed output of all reduce" must {

    val idx = 1
    val thReduce = 1f
    val thComplete = 1f
    val maxLag = 5
    val dataSize = 3
    val maxMsgSize = 2
    val numActors = 2

    val generator = (idx: Int, iter: Int) => idx + iter.toFloat

    "sum up all correct data" in {

      val source = createCustomDataSource(dataSize)(generator)

      val output1 = Array.range(0, dataSize).map(generator(_, 0)).map(_ * numActors).toList
      val output2 = Array.range(0, dataSize).map(generator(_, 1)).map(_ * numActors).toList
      val sink = assertiveDataSink(List(output1, output2), List(0, 1))


      val worker = createNewWorker(source, sink)

      // Replace test actor with the worker itself, it can actually send message to self - not intercepted by testactor
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(numActors).updated(idx, worker)

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxMsgSize)

      worker ! StartAllreduce(0)
      worker ! ScatterBlock(Array(2f), srcId = 0, destId = 1, chunkId = 0, 0)
      worker ! ReduceBlock(Array(0f, 2f), srcId = 0, destId = 1, chunkId =0, 0)

      worker ! StartAllreduce(1)
      worker ! ScatterBlock(Array(3f), srcId = 0, destId = 1, chunkId = 0, 1)
      worker ! ReduceBlock(Array(2f, 4f), srcId = 0, destId = 1, chunkId =0, 1)

      fishForMessage() {
        case CompleteAllreduce(1, 0) => true
        case _ => false
      }
      expectMsg(CompleteAllreduce(1, 1))
    }

  }

  "Early receiving reduce" must {

    val idx = 0
    val thReduce = 1f
    val thComplete = 0.8f
    val maxLag = 5
    val dataSize = 8
    val maxMsgSize = 2;
    val numActors = 4;

    val worker = createNewWorker(source, sink)
    val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(numActors)
    val futureRound = 3



    "send complete to master" in {

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxMsgSize)
      worker ! StartAllreduce(0)

      worker ! ReduceBlock(Array(12f, 15f), 0, 0, 0, futureRound)
      worker ! ReduceBlock(Array(11f, 10f), 1, 0, 0, futureRound)
      worker ! ReduceBlock(Array(10f, 20f), 2, 0, 0, futureRound)
      worker ! ReduceBlock(Array(9f, 10f), 3, 0, 0, futureRound)

      fishForMessage() {
        case c: CompleteAllreduce => {
          c.round shouldBe futureRound
          c.srcId shouldBe 0
          true
        }
        case _: ScatterBlock => false
      }
    }

    "no longer act on completed scatter for that round" in {
      for (i <- 0 until 4) {
        worker ! ScatterBlock(Array(2f * i, 2f * i), srcId = i, destId = 0, chunkId = 0, futureRound)
      }
      expectNoMsg()
    }
  }


  "Allreduce worker" must {
    "single-round allreduce" in {
      val worker = createNewWorker(source, sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1f
      val thComplete = 0.75f
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2
      val numActors = 4

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("============start normal test!===========")
      worker ! StartAllreduce(0)

      // expect scattering to other nodes
      for (i <- 0 until 4) {
        expectScatter(ScatterBlock(Array(2f * i, 2f * i + 1), srcId = 0, destId = i, 0, 0))
      }

      // simulate sending scatter from other nodes
      for (i <- 0 until 4) {
        worker ! ScatterBlock(Array(2f * i, 2f * i), srcId = i, destId = 0, 0, 0)
      }

      // expect sending reduced result to other nodes
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 3, 0, 0))

      // simulate sending reduced block from other nodes
      worker ! ReduceBlock(Array(12f, 15f), 0, 0, 0, 0)
      worker ! ReduceBlock(Array(11f, 10f), 1, 0, 0, 0)
      worker ! ReduceBlock(Array(10f, 20f), 2, 0, 0, 0)
      worker ! ReduceBlock(Array(9f, 10f), 3, 0, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
    }


    "single-round allreduce with nasty chunk size" in {

      val dataSize = 6
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(2)
      val idx = 0
      val thReduce = 0.9f
      val thComplete = 0.8f
      val maxLag = 5
      val maxChunkSize = 2
      val numActors = 2

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("============start normal test!===========")
      worker ! StartAllreduce(0)

      // expect scattering to other nodes
      expectScatter(ScatterBlock(Array(0f, 1f), srcId = 0, destId = 0, 0, 0))
      expectScatter(ScatterBlock(Array(2f), srcId = 0, destId = 0, 1, 0))
      expectScatter(ScatterBlock(Array(3f, 4f), srcId = 0, destId = 1, 0, 0))
      expectScatter(ScatterBlock(Array(5f), srcId = 0, destId = 1, 1, 0))

      // // simulate sending scatter block from other nodes
      worker ! ScatterBlock(Array(0f, 1f), srcId = 0, destId = 0, 0, 0)
      worker ! ScatterBlock(Array(2f), srcId = 0, destId = 0, 1, 0)
      worker ! ScatterBlock(Array(0f, 1f), srcId = 1, destId = 0, 0, 0)
      worker ! ScatterBlock(Array(2f), srcId = 1, destId = 0, 1, 0)


      // expect sending reduced result to other nodes
      expectReduce(ReduceBlock(Array(0f, 1f), srcId = 0, destId = 0, 0, 0))
      expectReduce(ReduceBlock(Array(0f, 1f), srcId = 0, destId = 1, 0, 0))
      expectReduce(ReduceBlock(Array(2f), srcId = 0, destId = 0, 1, 0))
      expectReduce(ReduceBlock(Array(2f), srcId = 0, destId = 1, 1, 0))

      // simulate sending reduced block from other nodes
      worker ! ReduceBlock(Array(0f, 2f), 0, 0, 0, 0)
      worker ! ReduceBlock(Array(4f), 0, 0, 1, 0)

      worker ! ReduceBlock(Array(6f, 8f), 1, 0, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
      worker ! ReduceBlock(Array(10f), 1, 0, 1, 0)


    }

    "single-round allreduce with nasty chunk size contd" in {
      val dataSize = 9
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(3)
      val idx = 0
      val thReduce = 0.7f
      val thComplete = 0.7f
      val maxLag = 5
      val maxChunkSize = 1
      val numActors = 2

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("============start normal test!===========")
      worker ! StartAllreduce(0)

      // expect scattering to other nodes
      expectScatter(ScatterBlock(Array(0f), srcId = 0, destId = 0, 0, 0))
      expectScatter(ScatterBlock(Array(1f), srcId = 0, destId = 0, 1, 0))
      expectScatter(ScatterBlock(Array(2f), srcId = 0, destId = 0, 2, 0))
      expectScatter(ScatterBlock(Array(3f), srcId = 0, destId = 1, 0, 0))
      expectScatter(ScatterBlock(Array(4f), srcId = 0, destId = 1, 1, 0))
      expectScatter(ScatterBlock(Array(5f), srcId = 0, destId = 1, 2, 0))
      expectScatter(ScatterBlock(Array(6f), srcId = 0, destId = 2, 0, 0))
      expectScatter(ScatterBlock(Array(7f), srcId = 0, destId = 2, 1, 0))
      expectScatter(ScatterBlock(Array(8f), srcId = 0, destId = 2, 2, 0))

      // // simulate sending scatter block from other nodes
      worker ! ScatterBlock(Array(0f), srcId = 0, destId = 0, 0, 0)
      worker ! ScatterBlock(Array(1f), srcId = 0, destId = 0, 1, 0)
      worker ! ScatterBlock(Array(2f), srcId = 0, destId = 0, 2, 0)
      worker ! ScatterBlock(Array(0f), srcId = 1, destId = 0, 0, 0)
      worker ! ScatterBlock(Array(1f), srcId = 1, destId = 0, 1, 0)
      worker ! ScatterBlock(Array(2f), srcId = 1, destId = 0, 2, 0)
      worker ! ScatterBlock(Array(0f), srcId = 2, destId = 0, 0, 0)
      worker ! ScatterBlock(Array(1f), srcId = 2, destId = 0, 1, 0)
      worker ! ScatterBlock(Array(2f), srcId = 2, destId = 0, 2, 0)


      // expect sending reduced result to other nodes
      //expectNoMsg()
      expectReduce(ReduceBlock(Array(0f), srcId = 0, destId = 0, 0, 0))
      expectReduce(ReduceBlock(Array(0f), srcId = 0, destId = 1, 0, 0))
      expectReduce(ReduceBlock(Array(0f), srcId = 0, destId = 2, 0, 0))
      expectReduce(ReduceBlock(Array(2f), srcId = 0, destId = 0, 1, 0))
      expectReduce(ReduceBlock(Array(2f), srcId = 0, destId = 1, 1, 0))
      expectReduce(ReduceBlock(Array(2f), srcId = 0, destId = 2, 1, 0))
      expectReduce(ReduceBlock(Array(4f), srcId = 0, destId = 0, 2, 0))
      expectReduce(ReduceBlock(Array(4f), srcId = 0, destId = 1, 2, 0))
      expectReduce(ReduceBlock(Array(4f), srcId = 0, destId = 2, 2, 0))


      // simulate sending reduced block from other nodes
      worker ! ReduceBlock(Array(0f), srcId = 0, destId = 0, 0, 0)
      worker ! ReduceBlock(Array(3f), srcId = 0, destId = 0, 1, 0)
      worker ! ReduceBlock(Array(6f), srcId = 0, destId = 0, 2, 0)
      worker ! ReduceBlock(Array(9f), srcId = 1, destId = 0, 0, 0)
      worker ! ReduceBlock(Array(12f), srcId = 1, destId = 0, 1, 0)
      worker ! ReduceBlock(Array(15f), srcId = 1, destId = 0, 2, 0)
      worker ! ReduceBlock(Array(18f), srcId = 2, destId = 0, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
      worker ! ReduceBlock(Array(21f), srcId = 2, destId = 0, 1, 0)
      worker ! ReduceBlock(Array(24f), srcId = 2, destId = 0, 2, 0)
      expectNoMsg()
    }

    "multi-round allreduce" in {
      val worker = createNewWorker(source, sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 0.8f
      val thComplete = 0.5f
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start multi-round test!==============")
      for (i <- 0 until 10) {
        worker ! StartAllreduce(i)
        expectScatter(ScatterBlock(Array(0f + i, 1f + i), 0, 0, 0, i))
        expectScatter(ScatterBlock(Array(2f + i, 3f + i), 0, 1, 0, i))
        expectScatter(ScatterBlock(Array(4f + i, 5f + i), 0, 2, 0, i))
        expectScatter(ScatterBlock(Array(6f + i, 7f + i), 0, 3, 0, i))
        worker ! ScatterBlock(Array(0f + i, 1f + i), 0, 0, 0, i)
        worker ! ScatterBlock(Array(0f + i, 1f + i), 1, 0, 0, i)
        worker ! ScatterBlock(Array(0f + i, 1f + i), 2, 0, 0, i)
        worker ! ScatterBlock(Array(0f + i, 1f + i), 3, 0, 0, i)
        expectReduce(ReduceBlock(Array(0f + 3 * i, 1f * 3 + 3 * i), 0, 0, 0, i))
        expectReduce(ReduceBlock(Array(0f + 3 * i, 1f * 3 + 3 * i), 0, 1, 0, i))
        expectReduce(ReduceBlock(Array(0f + 3 * i, 1f * 3 + 3 * i), 0, 2, 0, i))
        expectReduce(ReduceBlock(Array(0f + 3 * i, 1f * 3 + 3 * i), 0, 3, 0, i))
        worker ! ReduceBlock(Array(1f, 2f), 0, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 1, 0, 0, i)
        expectMsg(CompleteAllreduce(0, i))
        worker ! ReduceBlock(Array(1f, 2f), 2, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 3, 0, 0, i)
        expectNoMsg();
      }
    }

    "multi-round allreduce v2" in {
      val worker = createNewWorker(source, sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(2)
      val idx = 0
      val thReduce = 0.6f
      val thComplete = 0.8f
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start multi-round test!==============")
      for (i <- 0 until 10) {
        worker ! StartAllreduce(i)
        expectScatter(ScatterBlock(Array(0f + i, 1f + i), 0, 0, 0, i))
        expectScatter(ScatterBlock(Array(2f + i, 3f + i), 0, 0, 1, i))
        expectScatter(ScatterBlock(Array(4f + i, 5f + i), 0, 1, 0, i))
        expectScatter(ScatterBlock(Array(6f + i, 7f + i), 0, 1, 1, i))
        worker ! ScatterBlock(Array(0f + i, 1f + i), 0, 0, 0, i)
        worker ! ScatterBlock(Array(2f + i, 3f + i), 0, 0, 1, i)
        worker ! ScatterBlock(Array(10f + i, 11f + i), 1, 0, 0, i)
        worker ! ScatterBlock(Array(12f + i, 13f + i), 1, 0, 1, i)
        expectReduce(ReduceBlock(Array(0f * 1 + 1 * i, 1f * 1 + 1 * i), 0, 0, 0, i))
        expectReduce(ReduceBlock(Array(0f * 1 + 1 * i, 1f * 1 + 1 * i), 0, 1, 0, i))
        expectReduce(ReduceBlock(Array(2f * 1 + 1 * i, 3f * 1 + 1 * i), 0, 0, 1, i))
        expectReduce(ReduceBlock(Array(2f * 1 + 1 * i, 3f * 1 + 1 * i), 0, 1, 1, i))
        worker ! ReduceBlock(Array(1f, 2f), 0, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 0, 0, 1, i)
        worker ! ReduceBlock(Array(1f, 2f), 1, 0, 0, i)
        expectMsg(CompleteAllreduce(0, i))
        worker ! ReduceBlock(Array(1f, 2f), 1, 0, 1, i)
        expectNoMsg()

      }
    }

    "missed scatter" in {
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 0.75f
      val thComplete = 0.75f
      val maxLag = 5
      val dataSize = 4
      val maxChunkSize = 2
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start outdated scatter test!==============")
      worker ! StartAllreduce(0)
      expectScatter(ScatterBlock(Array(0f), 0, 0, 0, 0))
      expectScatter(ScatterBlock(Array(1f), 0, 1, 0, 0))
      expectScatter(ScatterBlock(Array(2f), 0, 2, 0, 0))
      expectScatter(ScatterBlock(Array(3f), 0, 3, 0, 0))
      worker ! ScatterBlock(Array(0f), 0, 0, 0, 0)
      expectNoMsg()
      worker ! ScatterBlock(Array(2f), 1, 0, 0, 0)
      expectNoMsg()
      worker ! ScatterBlock(Array(4f), 2, 0, 0, 0)
      worker ! ScatterBlock(Array(6f), 3, 0, 0, 0)

      expectReduce(ReduceBlock(Array(6f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 3, 0, 0))
      worker ! ReduceBlock(Array(12f), 0, 0, 0, 0)
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 0)
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
      worker ! ReduceBlock(Array(9f), 3, 0, 0, 0)
      expectNoMsg()
    }

    "future scatter" in {
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 0.75f
      val thComplete = 0.75f
      val maxLag = 5
      val dataSize = 4
      val maxChunkSize = 2
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start missing test!==============")
      worker ! StartAllreduce(0)
      expectScatter(ScatterBlock(Array(0f), 0, 0, 0, 0))
      expectScatter(ScatterBlock(Array(1f), 0, 1, 0, 0))
      expectScatter(ScatterBlock(Array(2f), 0, 2, 0, 0))
      expectScatter(ScatterBlock(Array(3f), 0, 3, 0, 0))

      worker ! ScatterBlock(Array(2f), 1, 0, 0, 0)
      worker ! ScatterBlock(Array(4f), 2, 0, 0, 0)
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 0)
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 0)
      // two of the messages is delayed, so now stall
      worker ! StartAllreduce(1) // master call it to do round 1
      worker ! ScatterBlock(Array(2f), 1, 0, 0, 1)
      worker ! ScatterBlock(Array(4f), 2, 0, 0, 1)
      worker ! ScatterBlock(Array(6f), 3, 0, 0, 1)

      expectScatter(ScatterBlock(Array(1f), 0, 0, 0, 1))
      expectScatter(ScatterBlock(Array(2f), 0, 1, 0, 1))
      expectScatter(ScatterBlock(Array(3f), 0, 2, 0, 1))
      expectScatter(ScatterBlock(Array(4f), 0, 3, 0, 1))
      expectReduce(ReduceBlock(Array(12f), 0, 0, 0, 1))
      expectReduce(ReduceBlock(Array(12f), 0, 1, 0, 1))
      expectReduce(ReduceBlock(Array(12f), 0, 2, 0, 1))
      expectReduce(ReduceBlock(Array(12f), 0, 3, 0, 1))
      // delayed message now get there
      worker ! ScatterBlock(Array(0f), 3, 0, 0, 0)
      worker ! ScatterBlock(Array(6f), 3, 0, 0, 0) // should be outdated
      expectReduce(ReduceBlock(Array(6f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(6f), 0, 3, 0, 0))
      println("finishing the reduce part")
      //worker ! ReduceBlock(Array(12), 0, 0, 1)

      worker ! ReduceBlock(Array(9f), 3, 0, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 1)
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 1)
      worker ! ReduceBlock(Array(9f), 3, 0, 0, 1)
      expectMsg(CompleteAllreduce(0, 1))
    }

    "missed reduce" in {
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1f
      val thComplete = 0.75f
      val dataSize = 4
      val maxChunkSize = 100
      val maxLag = 5
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start missing test!==============")
      worker ! StartAllreduce(0)
      expectScatter(ScatterBlock(Array(0f), 0, 0, 0, 0))
      expectScatter(ScatterBlock(Array(1f), 0, 1, 0, 0))
      expectScatter(ScatterBlock(Array(2f), 0, 2, 0, 0))
      expectScatter(ScatterBlock(Array(3f), 0, 3, 0, 0))
      worker ! ScatterBlock(Array(0f), 0, 0, 0, 0)
      worker ! ScatterBlock(Array(2f), 1, 0, 0, 0)
      worker ! ScatterBlock(Array(4f), 2, 0, 0, 0)
      worker ! ScatterBlock(Array(6f), 3, 0, 0, 0)
      expectReduce(ReduceBlock(Array(12f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 3, 0, 0))
      worker ! ReduceBlock(Array(12f), 0, 0, 0, 0)
      expectNoMsg()
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 0)
      expectNoMsg()
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 0)
      //worker ! ReduceBlock(Array(9), 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
    }

    "delayed future reduce" in {
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 0.75f
      val thComplete = 0.75f
      val dataSize = 4
      val maxChunkSize = 100
      val maxLag = 5
      val worker = createNewWorker(createBasicDataSource(4), sink)

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start delayed future reduce test!==============")
      worker ! StartAllreduce(0)
      expectScatter(ScatterBlock(Array(0f), 0, 0, 0, 0))
      expectScatter(ScatterBlock(Array(1f), 0, 1, 0, 0))
      expectScatter(ScatterBlock(Array(2f), 0, 2, 0, 0))
      expectScatter(ScatterBlock(Array(3f), 0, 3, 0, 0))

      worker ! ScatterBlock(Array(2f), 1, 0, 0, 0)
      worker ! ScatterBlock(Array(4f), 2, 0, 0, 0)
      worker ! ScatterBlock(Array(6f), 3, 0, 0, 0)
      expectReduce(ReduceBlock(Array(12f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(12f), 0, 3, 0, 0))
      worker ! StartAllreduce(1) // master call it to do round 1
      worker ! ScatterBlock(Array(3f), 1, 0, 0, 1)
      worker ! ScatterBlock(Array(5f), 2, 0, 0, 1)
      worker ! ScatterBlock(Array(7f), 3, 0, 0, 1)
      // we send scatter value of round 1 to peers in case someone need it
      expectScatter(ScatterBlock(Array(1f), 0, 0, 0, 1))
      expectScatter(ScatterBlock(Array(2f), 0, 1, 0, 1))
      expectScatter(ScatterBlock(Array(3f), 0, 2, 0, 1))
      expectScatter(ScatterBlock(Array(4f), 0, 3, 0, 1))
      expectReduce(ReduceBlock(Array(15f), 0, 0, 0, 1))
      expectReduce(ReduceBlock(Array(15f), 0, 1, 0, 1))
      expectReduce(ReduceBlock(Array(15f), 0, 2, 0, 1))
      expectReduce(ReduceBlock(Array(15f), 0, 3, 0, 1))
      println("finishing the reduce part")
      // assertion: reduce t would never come after reduce t+1. (FIFO of message) otherwise would fail!
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 0)
      worker ! ReduceBlock(Array(11f), 1, 0, 0, 1)
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 0)
      worker ! ReduceBlock(Array(10f), 2, 0, 0, 1)
      worker ! ReduceBlock(Array(9f), 3, 0, 0, 0)
      worker ! ReduceBlock(Array(9f), 3, 0, 0, 1)
      expectMsg(CompleteAllreduce(0, 0))
      expectMsg(CompleteAllreduce(0, 1))
    }

  }

  "Catch up" must {

    "simple catchup" in {
      val worker = createNewWorker(source, sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1
      val thComplete = 1
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start simple catchup test!==============")
      for (i <- 0 until 6) {
        worker ! StartAllreduce(i)
        expectBasicSendingScatterBlock(worker, i)

        simulateScatterBlocksFromPeers(worker, i)
        worker ! ReduceBlock(Array(12.0f, 12.0f), 1, 0, 0, i)
        worker ! ReduceBlock(Array(12.0f, 12.0f), 2, 0, 0, i)
        worker ! ReduceBlock(Array(12.0f, 12.0f), 3, 0, 0, i)
      }

      testCatchup(worker, maxLag, catchupRound = 6)
      testCatchup(worker, maxLag, catchupRound = 7)
      testCatchup(worker, maxLag, catchupRound = 8)
    }

    "cold catchup" in {
      // extreme case of catch up, only for test use
      val worker = createNewWorker(source, sink)
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1
      val thComplete = 1
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      worker ! StartAllreduce(10) // trigger the problem instantly
      // NOTE: here we need to nullify the reduce message instead.
      for (i <- 0 until 5) {
        expectReduce(ReduceBlock(Array(0f, 0f), 0, 0, 0, i))
        expectReduce(ReduceBlock(Array(0f, 0f), 0, 1, 0, i))
        expectReduce(ReduceBlock(Array(0f, 0f), 0, 2, 0, i))
        expectReduce(ReduceBlock(Array(0f, 0f), 0, 3, 0, i))
        expectMsg(CompleteAllreduce(0, i))
      }
      for (i <- 0 to 10) {
        expectBasicSendingScatterBlock(worker, i)
      }
    }

    "buffer when unitialized" in {

      val worker = createNewWorker(source, sink)
      worker ! StartAllreduce(0)
      expectNoMsg()
      val idx = 0
      val thReduce = 1
      val thComplete = 1
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2
      val workers = initializeWorkersAsSelf(4)
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      expectBasicSendingScatterBlock(worker, 0)

    }

  }


  "Sanity Check" must {

    "multi-round allreduce v3" in {
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(3)
      val idx = 0
      val thReduce = 0.75f
      val thComplete = 0.75f
      val dataSize = 9
      val maxChunkSize = 2
      val maxLag = 5
      val worker = createNewWorker(createBasicDataSource(dataSize), sink)

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      println("===============start delayed future reduce test!==============")
      worker ! StartAllreduce(0)
      expectScatter(ScatterBlock(Array(0f, 1f), 0, 0, 0, 0))
      expectScatter(ScatterBlock(Array(2f), 0, 0, 1, 0))
      expectScatter(ScatterBlock(Array(3f, 4f), 0, 1, 0, 0))
      expectScatter(ScatterBlock(Array(5f), 0, 1, 1, 0))
      expectScatter(ScatterBlock(Array(6f, 7f), 0, 2, 0, 0))
      expectScatter(ScatterBlock(Array(8f), 0, 2, 1, 0))

      worker ! ScatterBlock(Array(0f, 1f), 0, 0, 0, 0)
      worker ! ScatterBlock(Array(0f, 1f), 1, 0, 0, 0)
      worker ! ScatterBlock(Array(0f, 1f), 2, 0, 0, 0)
      worker ! ScatterBlock(Array(2f), 0, 0, 1, 0)
      worker ! ScatterBlock(Array(2f), 1, 0, 1, 0)
      worker ! ScatterBlock(Array(2f), 2, 0, 1, 0)

      expectReduce(ReduceBlock(Array(0f, 2f), 0, 0, 0, 0))
      expectReduce(ReduceBlock(Array(0f, 2f), 0, 1, 0, 0))
      expectReduce(ReduceBlock(Array(0f, 2f), 0, 2, 0, 0))
      expectReduce(ReduceBlock(Array(4f), 0, 0, 1, 0))
      expectReduce(ReduceBlock(Array(4f), 0, 1, 1, 0))
      expectReduce(ReduceBlock(Array(4f), 0, 2, 1, 0))

      worker ! StartAllreduce(1) // master call it to do round 1
      worker ! ScatterBlock(Array(10f, 11f), 1, 0, 0, 1)
      worker ! ScatterBlock(Array(12f), 1, 0, 1, 1)
      worker ! ScatterBlock(Array(10f, 11f), 2, 0, 0, 1)
      worker ! ScatterBlock(Array(12f), 2, 0, 1, 1)

      // we send scatter value of round 1 to peers in case someone need it
      expectScatter(ScatterBlock(Array(1f, 2f), 0, 0, 0, 1))
      expectScatter(ScatterBlock(Array(3f), 0, 0, 1, 1))
      expectScatter(ScatterBlock(Array(4f, 5f), 0, 1, 0, 1))
      expectScatter(ScatterBlock(Array(6f), 0, 1, 1, 1))
      expectScatter(ScatterBlock(Array(7f, 8f), 0, 2, 0, 1))
      expectScatter(ScatterBlock(Array(9f), 0, 2, 1, 1))

      expectReduce(ReduceBlock(Array(20f, 22f), 0, 0, 0, 1))
      expectReduce(ReduceBlock(Array(20f, 22f), 0, 1, 0, 1))
      expectReduce(ReduceBlock(Array(20f, 22f), 0, 2, 0, 1))
      expectReduce(ReduceBlock(Array(24f), 0, 0, 1, 1))
      expectReduce(ReduceBlock(Array(24f), 0, 1, 1, 1))
      expectReduce(ReduceBlock(Array(24f), 0, 2, 1, 1))
      println("finishing the reduce part")

      // assertion: reduce t would never come after reduce t+1. (FIFO of message) otherwise would fail!
      worker ! ReduceBlock(Array(11f, 11f), 1, 0, 0, 0)
      worker ! ReduceBlock(Array(11f), 1, 0, 1, 1)
      worker ! ReduceBlock(Array(11f, 11f), 1, 0, 0, 1)
      worker ! ReduceBlock(Array(11f), 1, 0, 1, 0)
      worker ! ReduceBlock(Array(11f, 11f), 2, 0, 0, 0)
      worker ! ReduceBlock(Array(11f), 2, 0, 1, 1)
      expectNoMsg()
      worker ! ReduceBlock(Array(11f, 11f), 2, 0, 0, 1)
      expectMsg(CompleteAllreduce(0, 1))
      worker ! ReduceBlock(Array(11f), 2, 0, 1, 0)
      expectMsg(CompleteAllreduce(0, 0))

    }
  }

  private def simulateScatterBlocksFromPeers(worker: ActorRef, i: Int) = {
    worker ! ScatterBlock(Array(1.0f * (i + 1), 1.0f * (i + 1)), 1, 0, 0, i)
    worker ! ScatterBlock(Array(2.0f * (i + 1), 2.0f * (i + 1)), 2, 0, 0, i)
    worker ! ScatterBlock(Array(4.0f * (i + 1), 4.0f * (i + 1)), 3, 0, 0, i)
  }

  private def expectBasicSendingScatterBlock(worker: ActorRef, i: Int) = {
    expectScatter(ScatterBlock(Array(0f + i, 1f + i), 0, 0, 0, i))
    expectScatter(ScatterBlock(Array(2f + i, 3f + i), 0, 1, 0, i))
    expectScatter(ScatterBlock(Array(4f + i, 5f + i), 0, 2, 0, i))
    expectScatter(ScatterBlock(Array(6f + i, 7f + i), 0, 3, 0, i))
  }

  private def testCatchup(worker: ActorRef, maxLag: Int, catchupRound: Int) = {
    worker ! StartAllreduce(catchupRound) // trigger the first round to catchup
    val completionRound = catchupRound - (maxLag + 1)

    expectBasicSendingReduceBlock(completionRound)
    expectMsg(CompleteAllreduce(0, completionRound))
    expectScatter(ScatterBlock(Array(0f + catchupRound, 1f + catchupRound), 0, 0, 0, catchupRound))
    expectScatter(ScatterBlock(Array(2f + catchupRound, 3f + catchupRound), 0, 1, 0, catchupRound))
    expectScatter(ScatterBlock(Array(4f + catchupRound, 5f + catchupRound), 0, 2, 0, catchupRound))
    expectScatter(ScatterBlock(Array(6f + catchupRound, 7f + catchupRound), 0, 3, 0, catchupRound))
  }

  private def expectBasicSendingReduceBlock(i: Int) = {
    expectReduce(ReduceBlock(Array(7.0f * (i + 1), 7.0f * (i + 1)), 0, 0, 0, i))
    expectReduce(ReduceBlock(Array(7.0f * (i + 1), 7.0f * (i + 1)), 0, 1, 0, i))
    expectReduce(ReduceBlock(Array(7.0f * (i + 1), 7.0f * (i + 1)), 0, 2, 0, i))
    expectReduce(ReduceBlock(Array(7.0f * (i + 1), 7.0f * (i + 1)), 0, 3, 0, i))
  }


  /**
    * Expect scatter block containing array value. This is needed because standard expectMsg will not be able to match
    * mutable Array and check for equality during assertion.
    */
  private def expectScatter(expected: ScatterBlock) = {
    receiveOne(remainingOrDefault) match {
      case s: ScatterBlock =>
        s.srcId shouldEqual expected.srcId
        s.destId shouldEqual expected.destId
        s.round shouldEqual expected.round
        s.value.toList shouldEqual expected.value.toList
        s.chunkId shouldEqual expected.chunkId
    }
  }

  /**
    * Expect reduce block containing array value. This is needed because standard expectMsg will not be able to match
    * mutable Array and check for equality during assertion.
    */
  private def expectReduce(expected: ReduceBlock) = {
    receiveOne(remainingOrDefault) match {
      case r: ReduceBlock =>
        r.srcId shouldEqual expected.srcId
        r.destId shouldEqual expected.destId
        r.round shouldEqual expected.round
        r.value.toList shouldEqual expected.value.toList
        r.chunkId shouldEqual expected.chunkId
    }
  }

  private def createNewWorker(source: DataSource, sink: DataSink) = {
    system.actorOf(
      Props(
        classOf[AllreduceWorker],
        source,
        sink
      ),
      name = Random.alphanumeric.take(10).mkString
    )
  }

  private def initializeWorkersAsSelf(size: Int): Map[Int, ActorRef] = {

    (for {
      i <- 0 until size
    } yield (i, self)).toMap

  }

}