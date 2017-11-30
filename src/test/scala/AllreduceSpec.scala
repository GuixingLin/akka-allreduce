// import akka.actor.{ActorRef, ActorSystem, Props}
// import akka.testkit.{ImplicitSender, TestKit}
// import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
// import sample.cluster.allreduce._

// import scala.util.Random

// class AllreduceSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
//   with WordSpecLike with Matchers with BeforeAndAfterAll {
//   override def afterAll {
//     TestKit.shutdownActorSystem(system)
//   }

//   // basic setup
//   val idx = 0
//   val thReduce = 1
//   val thComplete = 0.75
//   val maxLag = 5
//   val dataSize = 8
//   val maxMsgSize = 2;

//   "Early receiving reduce" must {

//     val worker = createNewWorker()
//     val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(2)
//     val futureRound = 3

//     "send complete to master" in {

//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxMsgSize)
//       worker ! StartAllreduce(0)

//       worker ! ReduceBlock(Array(12, 15), 0, 0, 0, futureRound)
//       worker ! ReduceBlock(Array(11, 10), 1, 0, 1, futureRound)
//       worker ! ReduceBlock(Array(10, 20), 2, 0, 2, futureRound)
//       worker ! ReduceBlock(Array(9, 10), 3, 0, 3, futureRound)

//       fishForMessage() {
//         case c: CompleteAllreduce => {
//           c.round shouldBe futureRound
//           c.srcId shouldBe 0
//           true
//         }
//         case _: ScatterBlock => false
//       }
//     }

//     "no longer act on completed scatter for that round" in {
//       for (i <- 0 until 4) {
//         worker ! ScatterBlock(Array(2.0 * i, 2.0 * i), srcId = i, destId = 0, chunkId = i, futureRound)
//       }
//       expectNoMsg()
//     }
//   }


//   "Allreduce worker" must {
//     "single-round allreduce" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 0.75
//       val maxLag = 5
//       val dataSize = 8
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, 8)
//       println("============start normal test!===========")
//       worker ! StartAllreduce(0)

//       // expect scattering to other nodes
//       for (i <- 0 until 4) {
//         expectScatter(ScatterBlock(Array(2.0 * i, 2.0 * i + 1), 0, i, 0))
//       }

//       // simulate sending scatter from other nodes
//       for (i <- 0 until 4) {
//         worker ! ScatterBlock(Array(2.0 * i, 2.0 * i), srcId = i, destId = 0, 0)
//       }

//       // expect sending reduced result to other nodes
//       expectReduce(ReduceBlock(Array(12, 12), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(12, 12), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(12, 12), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(12, 12), 0, 3, 0))

//       // simulate sending reduced block from other nodes
//       worker ! ReduceBlock(Array(12, 15), 0, 0, 0)
//       worker ! ReduceBlock(Array(11, 10), 1, 0, 0)
//       worker ! ReduceBlock(Array(10, 20), 2, 0, 0)
//       worker ! ReduceBlock(Array(9, 10), 3, 0, 0)
//       expectMsg(CompleteAllreduce(0, 0))
//     }

//     "multi-round allreduce" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 0.75
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start multi-round test!==============")
//       for (i <- 0 until 10) {
//         worker ! StartAllreduce(i)
//         expectScatter(ScatterBlock(Array(0.0 + i), 0, 0, i))
//         expectScatter(ScatterBlock(Array(1.0 + i), 0, 1, i))
//         expectScatter(ScatterBlock(Array(2.0 + i), 0, 2, i))
//         expectScatter(ScatterBlock(Array(3.0 + i), 0, 3, i))
//         worker ! ScatterBlock(Array(0.0), 0, 0, i)
//         worker ! ScatterBlock(Array(2.0), 1, 0, i)
//         worker ! ScatterBlock(Array(4.0), 2, 0, i)
//         worker ! ScatterBlock(Array(6.0), 3, 0, i)
//         expectReduce(ReduceBlock(Array(12), 0, 0, i))
//         expectReduce(ReduceBlock(Array(12), 0, 1, i))
//         expectReduce(ReduceBlock(Array(12), 0, 2, i))
//         expectReduce(ReduceBlock(Array(12), 0, 3, i))
//         worker ! ReduceBlock(Array(12), 0, 0, i)
//         worker ! ReduceBlock(Array(11), 1, 0, i)
//         worker ! ReduceBlock(Array(10), 2, 0, i)
//         worker ! ReduceBlock(Array(9), 3, 0, i)
//         expectMsg(CompleteAllreduce(0, i))
//       }
//     }
//     "missed scatter" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 0.75
//       val thComplete = 0.75
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start outdated scatter test!==============")
//       worker ! StartAllreduce(0)
//       expectScatter(ScatterBlock(Array(0.0), 0, 0, 0))
//       expectScatter(ScatterBlock(Array(1.0), 0, 1, 0))
//       expectScatter(ScatterBlock(Array(2.0), 0, 2, 0))
//       expectScatter(ScatterBlock(Array(3.0), 0, 3, 0))
//       worker ! ScatterBlock(Array(0.0), 0, 0, 0)
//       expectNoMsg()
//       worker ! ScatterBlock(Array(2.0), 1, 0, 0)
//       expectNoMsg()
//       worker ! ScatterBlock(Array(4.0), 2, 0, 0)
//       //worker ! ScatterBlock(Array(6.0), 3, 0, 0)
//       expectReduce(ReduceBlock(Array(6), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 3, 0))
//       worker ! ReduceBlock(Array(12), 0, 0, 0)
//       worker ! ReduceBlock(Array(11), 1, 0, 0)
//       worker ! ReduceBlock(Array(10), 2, 0, 0)
//       worker ! ReduceBlock(Array(9), 3, 0, 0)
//       expectMsg(CompleteAllreduce(0, 0))
//     }
//     "missed reduce" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 0.75
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start missing test!==============")
//       worker ! StartAllreduce(0)
//       expectScatter(ScatterBlock(Array(0.0), 0, 0, 0))
//       expectScatter(ScatterBlock(Array(1.0), 0, 1, 0))
//       expectScatter(ScatterBlock(Array(2.0), 0, 2, 0))
//       expectScatter(ScatterBlock(Array(3.0), 0, 3, 0))
//       worker ! ScatterBlock(Array(0.0), 0, 0, 0)
//       worker ! ScatterBlock(Array(2.0), 1, 0, 0)
//       worker ! ScatterBlock(Array(4.0), 2, 0, 0)
//       worker ! ScatterBlock(Array(6.0), 3, 0, 0)
//       expectReduce(ReduceBlock(Array(12), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 3, 0))
//       worker ! ReduceBlock(Array(12), 0, 0, 0)
//       expectNoMsg()
//       worker ! ReduceBlock(Array(11), 1, 0, 0)
//       expectNoMsg()
//       worker ! ReduceBlock(Array(10), 2, 0, 0)
//       //worker ! ReduceBlock(Array(9), 3, 0, 0)
//       expectMsg(CompleteAllreduce(0, 0))
//     }
//     "future scatter" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 0.75
//       val thComplete = 0.75
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start missing test!==============")
//       worker ! StartAllreduce(0)
//       expectScatter(ScatterBlock(Array(0.0), 0, 0, 0))
//       expectScatter(ScatterBlock(Array(1.0), 0, 1, 0))
//       expectScatter(ScatterBlock(Array(2.0), 0, 2, 0))
//       expectScatter(ScatterBlock(Array(3.0), 0, 3, 0))

//       worker ! ScatterBlock(Array(2.0), 1, 0, 0)
//       worker ! ScatterBlock(Array(4.0), 2, 0, 0)
//       worker ! ReduceBlock(Array(11), 1, 0, 0)
//       worker ! ReduceBlock(Array(10), 2, 0, 0)
//       // two of the messages is delayed, so now stall
//       worker ! StartAllreduce(1) // master call it to do round 1
//       worker ! ScatterBlock(Array(2.0), 1, 0, 1)
//       worker ! ScatterBlock(Array(4.0), 2, 0, 1)
//       worker ! ScatterBlock(Array(6.0), 3, 0, 1)

//       expectScatter(ScatterBlock(Array(1.0), 0, 0, 1))
//       expectScatter(ScatterBlock(Array(2.0), 0, 1, 1))
//       expectScatter(ScatterBlock(Array(3.0), 0, 2, 1))
//       expectScatter(ScatterBlock(Array(4.0), 0, 3, 1))
//       expectReduce(ReduceBlock(Array(12), 0, 0, 1))
//       expectReduce(ReduceBlock(Array(12), 0, 1, 1))
//       expectReduce(ReduceBlock(Array(12), 0, 2, 1))
//       expectReduce(ReduceBlock(Array(12), 0, 3, 1))
//       // delayed message now get there
//       worker ! ScatterBlock(Array(0.0), 3, 0, 0)
//       worker ! ScatterBlock(Array(6.0), 3, 0, 0) // should be outdated
//       expectReduce(ReduceBlock(Array(6), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(6), 0, 3, 0))
//       println("finishing the reduce part")
//       //worker ! ReduceBlock(Array(12), 0, 0, 1)

//       worker ! ReduceBlock(Array(9), 3, 0, 0)
//       expectMsg(CompleteAllreduce(0, 0))
//       worker ! ReduceBlock(Array(11), 1, 0, 1)
//       worker ! ReduceBlock(Array(10), 2, 0, 1)
//       worker ! ReduceBlock(Array(9), 3, 0, 1)
//       expectMsg(CompleteAllreduce(0, 1))
//     }
//     "delayed future reduce" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 0.75
//       val thComplete = 0.75
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start delayed future reduce test!==============")
//       worker ! StartAllreduce(0)
//       expectScatter(ScatterBlock(Array(0.0), 0, 0, 0))
//       expectScatter(ScatterBlock(Array(1.0), 0, 1, 0))
//       expectScatter(ScatterBlock(Array(2.0), 0, 2, 0))
//       expectScatter(ScatterBlock(Array(3.0), 0, 3, 0))

//       worker ! ScatterBlock(Array(2.0), 1, 0, 0)
//       worker ! ScatterBlock(Array(4.0), 2, 0, 0)
//       worker ! ScatterBlock(Array(6.0), 3, 0, 0)
//       expectReduce(ReduceBlock(Array(12), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(12), 0, 3, 0))
//       worker ! StartAllreduce(1) // master call it to do round 1
//       worker ! ScatterBlock(Array(3.0), 1, 0, 1)
//       worker ! ScatterBlock(Array(5.0), 2, 0, 1)
//       worker ! ScatterBlock(Array(7.0), 3, 0, 1)
//       // we send scatter value of round 1 to peers in case someone need it
//       expectScatter(ScatterBlock(Array(1.0), 0, 0, 1))
//       expectScatter(ScatterBlock(Array(2.0), 0, 1, 1))
//       expectScatter(ScatterBlock(Array(3.0), 0, 2, 1))
//       expectScatter(ScatterBlock(Array(4.0), 0, 3, 1))
//       expectReduce(ReduceBlock(Array(15), 0, 0, 1))
//       expectReduce(ReduceBlock(Array(15), 0, 1, 1))
//       expectReduce(ReduceBlock(Array(15), 0, 2, 1))
//       expectReduce(ReduceBlock(Array(15), 0, 3, 1))
//       println("finishing the reduce part")
//       // assertion: reduce t would never come after reduce t+1. (FIFO of message) otherwise would fail!
//       worker ! ReduceBlock(Array(11), 1, 0, 0)
//       worker ! ReduceBlock(Array(11), 1, 0, 1)
//       worker ! ReduceBlock(Array(10), 2, 0, 0)
//       worker ! ReduceBlock(Array(10), 2, 0, 1)
//       worker ! ReduceBlock(Array(9), 3, 0, 0)
//       worker ! ReduceBlock(Array(9), 3, 0, 1)
//       expectMsg(CompleteAllreduce(0, 0))
//       expectMsg(CompleteAllreduce(0, 1))
//     }
//     "simple catchup" in {
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 1
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       println("===============start simple catchup test!==============")
//       for (i <- 0 until 6) {
//         worker ! StartAllreduce(i)
//         expectScatter(ScatterBlock(Array(0 + i), 0, 0, i))
//         expectScatter(ScatterBlock(Array(1 + i), 0, 1, i))
//         expectScatter(ScatterBlock(Array(2 + i), 0, 2, i))
//         expectScatter(ScatterBlock(Array(3 + i), 0, 3, i))
//         worker ! ScatterBlock(Array(1.0), 1, 0, i)
//         worker ! ScatterBlock(Array(2.0), 2, 0, i)
//         worker ! ScatterBlock(Array(4.0), 3, 0, i)
//         worker ! ReduceBlock(Array(12), 1, 0, i)
//         worker ! ReduceBlock(Array(11), 2, 0, i)
//         worker ! ReduceBlock(Array(10), 3, 0, i)
//       }
//       worker ! StartAllreduce(6) // trigger the first round to catchup
//       expectReduce(ReduceBlock(Array(7.0), 0, 0, 0))
//       expectReduce(ReduceBlock(Array(7.0), 0, 1, 0))
//       expectReduce(ReduceBlock(Array(7.0), 0, 2, 0))
//       expectReduce(ReduceBlock(Array(7.0), 0, 3, 0))
//       expectMsg(CompleteAllreduce(0, 0))
//       expectScatter(ScatterBlock(Array(0 + 6), 0, 0, 6))
//       expectScatter(ScatterBlock(Array(1 + 6), 0, 1, 6))
//       expectScatter(ScatterBlock(Array(2 + 6), 0, 2, 6))
//       expectScatter(ScatterBlock(Array(3 + 6), 0, 3, 6))
//       worker ! StartAllreduce(7) // test another round to make sure it works
//       expectReduce(ReduceBlock(Array(7.0), 0, 0, 1))
//       expectReduce(ReduceBlock(Array(7.0), 0, 1, 1))
//       expectReduce(ReduceBlock(Array(7.0), 0, 2, 1))
//       expectReduce(ReduceBlock(Array(7.0), 0, 3, 1))
//       expectMsg(CompleteAllreduce(0, 1))
//       expectScatter(ScatterBlock(Array(0 + 7), 0, 0, 7))
//       expectScatter(ScatterBlock(Array(1 + 7), 0, 1, 7))
//       expectScatter(ScatterBlock(Array(2 + 7), 0, 2, 7))
//       expectScatter(ScatterBlock(Array(3 + 7), 0, 3, 7))
//     }
//     "cold catchup" in {
//       // extreme case of catch up, only for test use
//       val worker = createNewWorker()
//       val workers: Map[Int, ActorRef] = initializeWorkersAsSelf(4)
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 1
//       val maxLag = 5
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       worker ! StartAllreduce(10) // trigger the problem instantly
//       // NOTE: here we need to nullify the reduce message instead.
//       for (i <- 0 until 5) {
//         expectReduce(ReduceBlock(Array(0), 0, 0, i))
//         expectReduce(ReduceBlock(Array(0), 0, 1, i))
//         expectReduce(ReduceBlock(Array(0), 0, 2, i))
//         expectReduce(ReduceBlock(Array(0), 0, 3, i))
//         expectMsg(CompleteAllreduce(0, i))

//       }
//       for (i <- 0 to 10) {
//         expectScatter(ScatterBlock(Array(0 + i), 0, 0, i))
//         expectScatter(ScatterBlock(Array(1 + i), 0, 1, i))
//         expectScatter(ScatterBlock(Array(2 + i), 0, 2, i))
//         expectScatter(ScatterBlock(Array(3 + i), 0, 3, i))
//       }
//     }

//     "buffer when unitialized" in {

//       val worker = createNewWorker()
//       worker ! StartAllreduce(0)
//       expectNoMsg()
//       val idx = 0
//       val thReduce = 1
//       val thComplete = 1
//       val maxLag = 5
//       val workers = initializeWorkersAsSelf(4)
//       worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, workers.size)
//       expectScatter(ScatterBlock(Array(0.0), 0, 0, 0))
//       expectScatter(ScatterBlock(Array(1.0), 0, 1, 0))
//       expectScatter(ScatterBlock(Array(2.0), 0, 2, 0))
//       expectScatter(ScatterBlock(Array(3.0), 0, 3, 0))

//     }

//   }

//   /**
//     * Expect scatter block containing array value. This is needed because standard expectMsg will not be able to match
//     * mutable Array and check for equality during assertion.
//     */
//   private def expectScatter(expected: ScatterBlock) = {
//     receiveOne(remainingOrDefault) match {
//       case s: ScatterBlock =>
//         s.srcId shouldEqual expected.srcId
//         s.destId shouldEqual expected.destId
//         s.round shouldEqual expected.round
//         s.value.toList shouldEqual expected.value.toList
//     }
//   }

//   /**
//     * Expect reduce block containing array value. This is needed because standard expectMsg will not be able to match
//     * mutable Array and check for equality during assertion.
//     */
//   private def expectReduce(expected: ReduceBlock) = {
//     receiveOne(remainingOrDefault) match {
//       case r: ReduceBlock =>
//         r.srcId shouldEqual expected.srcId
//         r.destId shouldEqual expected.destId
//         r.round shouldEqual expected.round
//         r.value.toList shouldEqual expected.value.toList
//     }
//   }


//   private def createNewWorker() = {
//     system.actorOf(Props[AllreduceWorker], name = Random.alphanumeric.take(10).mkString)
//   }

//   private def initializeWorkersAsSelf(size: Int): Map[Int, ActorRef] = {

//     (for {
//       i <- 0 until size
//     } yield (i, self)).toMap

//   }

// }