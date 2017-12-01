import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import sample.cluster.allreduce._

import scala.util.Random

class AllReduceSpec_v2() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }


  "Early receiving reduce" must {

    val idx = 0
    val thReduce = 1f
    val thComplete = 0.8f
    val maxLag = 5
    val dataSize = 8
    val maxMsgSize = 2;
    val numActors = 4;

    val worker = createNewWorker()
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
      val worker = createNewWorker()
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
      val worker = createNewWorker()
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(2)
      val idx = 0
      val thReduce = 0.9f
      val thComplete = 0.8f
      val maxLag = 5
      val dataSize = 6
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
      val worker = createNewWorker()
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(3)
      val idx = 0
      val thReduce = 0.7f
      val thComplete = 0.7f
      val maxLag = 5
      val dataSize = 9
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
      val worker = createNewWorker()
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1f
      val thComplete = 1f
      val maxLag = 5
      val dataSize = 8
      val maxChunkSize = 2
      val numActors = 2

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
        expectReduce(ReduceBlock(Array(0f + 4 * i, 1f * 4 + 4 * i), 0, 0, 0, i))
        expectReduce(ReduceBlock(Array(0f + 4 * i, 1f * 4 + 4 * i), 0, 1, 0, i))
        expectReduce(ReduceBlock(Array(0f + 4 * i, 1f * 4 + 4 * i), 0, 2, 0, i))
        expectReduce(ReduceBlock(Array(0f + 4 * i, 1f * 4 + 4 * i), 0, 3, 0, i))
        worker ! ReduceBlock(Array(1f, 2f), 0, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 1, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 2, 0, 0, i)
        worker ! ReduceBlock(Array(1f, 2f), 3, 0, 0, i)
        expectMsg(CompleteAllreduce(0, i))
      }
    }
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


  private def createNewWorker() = {
    system.actorOf(Props[AllreduceWorker], name = Random.alphanumeric.take(10).mkString)
  }

  private def initializeWorkersAsSelf(size: Int): Map[Int, ActorRef] = {

    (for {
      i <- 0 until size
    } yield (i, self)).toMap

  }

}