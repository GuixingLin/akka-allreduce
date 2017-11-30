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

  // basic setup
  val idx = 0
  val thReduce = 1f
  val thComplete = 0.75f
  val maxLag = 5
  val dataSize = 8
  val maxMsgSize = 2;
  val numActors = 4;

/*
  "Early receiving reduce" must {

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
*/

  "Allreduce worker" must {
    "single-round allreduce" in {
      val worker = createNewWorker()
      val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
      val idx = 0
      val thReduce = 1f
      val thComplete = 0.75f
      val maxLag = 5
      val dataSize = 8
      val maxMsgSize = 2
      val numActors = 4

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxMsgSize)
      println("============start normal test!===========")
      worker ! StartAllreduce(0)

      // expect scattering to other nodes
      for (i <- 0 until 4) {
        expectScatter(ScatterBlock(Array(2f * i, 2f * i + 1), 0, i, 0))
      }

      // simulate sending scatter from other nodes
      for (i <- 0 until 4) {
        worker ! ScatterBlock(Array(2f * i, 2f * i), srcId = i, destId = 0, 0)
      }

      // expect sending reduced result to other nodes
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 0, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 1, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 2, 0))
      expectReduce(ReduceBlock(Array(12f, 12f), 0, 3, 0))

      // simulate sending reduced block from other nodes
      worker ! ReduceBlock(Array(12f, 15f), 0, 0, 0)
      worker ! ReduceBlock(Array(11f, 10f), 1, 0, 0)
      worker ! ReduceBlock(Array(10f, 20f), 2, 0, 0)
      worker ! ReduceBlock(Array(9f, 10f), 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
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