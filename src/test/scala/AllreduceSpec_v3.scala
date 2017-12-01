import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import sample.cluster.allreduce._

class AllReduceSpec_v3() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
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

  type DataSink = AllReduceOutput => Unit
  type DataSource = AllReduceInputRequest => AllReduceInput

  val source: DataSource = req => {
    val floats = Array.range(0, dataSize).map(_ * (1 + req.iteration).toFloat)
    println(s"Data source at #${req.iteration}: ${floats.toList}")
    AllReduceInput(floats)
  }
  val sink: DataSink = r => {
    println(s"Data output at #${r.iteration}: ${r.data.toList}")
  }

  def assertSinkAtIterations(expected: List[List[Float]], expectedIterations: List[Int]): DataSink = {
    r => {
      val pos = expectedIterations.indexOf(r.iteration)
      pos >= 0 shouldBe true
      r.data shouldEqual expected(pos)
    }

  }


  "Early receiving reduce" must {

    val expectedFlushedData = List(12, 15, 11, 10, 10, 20, 9, 10).map(_.toFloat)
    val worker = createNewWorker(source, assertSinkAtIterations(List(expectedFlushedData), List(3)))
    val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(numActors)
    val thComplete = 1
    val futureRound = 3

    "send complete to master" in {

      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxMsgSize)
      worker ! StartAllreduce(0)

      for (i <- 0 until 4) {
        expectScatter(ScatterBlock(Array(2f * i, 2f * i + 1), 0, i, chunkId = 0, round = 0))
      }

      worker ! ReduceBlock(Array(12f, 15f), 0, 0, 0, futureRound)
      worker ! ReduceBlock(Array(11f, 10f), 1, 0, 0, futureRound)
      worker ! ReduceBlock(Array(10f, 20f), 2, 0, 0, futureRound)
      worker ! ReduceBlock(Array(9f, 10f), 3, 0, 0, futureRound)

      fishForMessage() {
        case c: CompleteAllreduce => {
          c.round shouldBe futureRound
          c.srcId shouldBe 0
          println(s"All reduce ${c.round} complete!")
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


  private def createNewWorker(source: DataSource, sink: DataSink) = {

    system.actorOf(
      Props(
        classOf[AllreduceWorker],
        source,
        sink
      ),
      name = "master"
    )
  }

  private def initializeWorkersAsSelf(size: Int): Map[Int, ActorRef] = {
    (for {
      i <- 0 until size
    } yield (i, self)).toMap

  }

}