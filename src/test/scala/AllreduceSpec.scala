import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import sample.cluster.allreduce._

import scala.collection.immutable.HashMap

class AllreduceSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Allreduce worker" must {
    "single-round allreduce" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("============start normal test!===========")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0, 0, 0, 0))
      expectMsg(Scatter(1.0, 0, 1, 0))
      expectMsg(Scatter(2.0, 0, 2, 0))
      expectMsg(Scatter(3.0, 0, 3, 0))
      worker ! Scatter(0.0, 0, 0, 0)
      worker ! Scatter(2.0, 1, 0, 0)
      worker ! Scatter(4.0, 2, 0, 0)
      worker ! Scatter(6.0, 3, 0, 0)
      expectMsg(Reduce(12, 0, 0, 0))
      expectMsg(Reduce(12, 0, 1, 0))
      expectMsg(Reduce(12, 0, 2, 0))
      expectMsg(Reduce(12, 0, 3, 0))
      worker ! Reduce(12, 0, 0, 0)
      worker ! Reduce(11, 1, 0, 0)
      worker ! Reduce(10, 2, 0, 0)
      worker ! Reduce(9, 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
    }
    "multi-round allreduce" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker2")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start multi-round test!==============")
      for (i <- 0 until 10) {
        worker ! StartAllreduce(i)
        expectMsg(Scatter(0.0 + i, 0, 0, i))
        expectMsg(Scatter(1.0 + i, 0, 1, i))
        expectMsg(Scatter(2.0 + i, 0, 2, i))
        expectMsg(Scatter(3.0 + i, 0, 3, i))
        worker ! Scatter(0.0, 0, 0, i)
        worker ! Scatter(2.0, 1, 0, i)
        worker ! Scatter(4.0, 2, 0, i)
        worker ! Scatter(6.0, 3, 0, i)
        expectMsg(Reduce(12, 0, 0, i))
        expectMsg(Reduce(12, 0, 1, i))
        expectMsg(Reduce(12, 0, 2, i))
        expectMsg(Reduce(12, 0, 3, i))
        worker ! Reduce(12, 0, 0, i)
        worker ! Reduce(11, 1, 0, i)
        worker ! Reduce(10, 2, 0, i)
        worker ! Reduce(9, 3, 0, i)
        expectMsg(CompleteAllreduce(0, i))
      }
    }
    "missed scatter" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker3")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 0.75
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start outdated scatter test!==============")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0, 0, 0, 0))
      expectMsg(Scatter(1.0, 0, 1, 0))
      expectMsg(Scatter(2.0, 0, 2, 0))
      expectMsg(Scatter(3.0, 0, 3, 0))
      worker ! Scatter(0.0, 0, 0, 0)
      expectNoMsg()
      worker ! Scatter(2.0, 1, 0, 0)
      expectNoMsg()
      worker ! Scatter(4.0, 2, 0, 0)
      //worker ! Scatter(6.0, 3, 0, 0)
      expectMsg(Reduce(6, 0, 0, 0))
      expectMsg(Reduce(6, 0, 1, 0))
      expectMsg(Reduce(6, 0, 2, 0))
      expectMsg(Reduce(6, 0, 3, 0))
      worker ! Reduce(12, 0, 0, 0)
      worker ! Reduce(11, 1, 0, 0)
      worker ! Reduce(10, 2, 0, 0)
      worker ! Reduce(9, 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
    }
    "missed reduce" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker4")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start missing test!==============")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0, 0, 0, 0))
      expectMsg(Scatter(1.0, 0, 1, 0))
      expectMsg(Scatter(2.0, 0, 2, 0))
      expectMsg(Scatter(3.0, 0, 3, 0))
      worker ! Scatter(0.0, 0, 0, 0)
      worker ! Scatter(2.0, 1, 0, 0)
      worker ! Scatter(4.0, 2, 0, 0)
      worker ! Scatter(6.0, 3, 0, 0)
      expectMsg(Reduce(12, 0, 0, 0))
      expectMsg(Reduce(12, 0, 1, 0))
      expectMsg(Reduce(12, 0, 2, 0))
      expectMsg(Reduce(12, 0, 3, 0))
      worker ! Reduce(12, 0, 0, 0)
      expectNoMsg()
      worker ! Reduce(11, 1, 0, 0)
      expectNoMsg()
      worker ! Reduce(10, 2, 0, 0)
      //worker ! Reduce(9, 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
    }
    "future scatter" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker5")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 0.75
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start missing test!==============")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0, 0, 0, 0))
      expectMsg(Scatter(1.0, 0, 1, 0))
      expectMsg(Scatter(2.0, 0, 2, 0))
      expectMsg(Scatter(3.0, 0, 3, 0))

      worker ! Scatter(2.0, 1, 0, 0)
      worker ! Scatter(4.0, 2, 0, 0)
      worker ! Reduce(11, 1, 0, 0)
      worker ! Reduce(10, 2, 0, 0)
      // two of the messages is delayed, so now stall
      worker ! StartAllreduce(1) // master call it to do round 1
      worker ! Scatter(2.0, 1, 0, 1)
      worker ! Scatter(4.0, 2, 0, 1)
      worker ! Scatter(6.0, 3, 0, 1)

      expectMsg(Scatter(1.0, 0, 0, 1))
      expectMsg(Scatter(2.0, 0, 1, 1))
      expectMsg(Scatter(3.0, 0, 2, 1))
      expectMsg(Scatter(4.0, 0, 3, 1))
      expectMsg(Reduce(12, 0, 0, 1))
      expectMsg(Reduce(12, 0, 1, 1))
      expectMsg(Reduce(12, 0, 2, 1))
      expectMsg(Reduce(12, 0, 3, 1))
      // delayed message now get there
      worker ! Scatter(0.0, 3, 0, 0)
      worker ! Scatter(6.0, 3, 0, 0) // should be outdated
      expectMsg(Reduce(6, 0, 0, 0))
      expectMsg(Reduce(6, 0, 1, 0))
      expectMsg(Reduce(6, 0, 2, 0))
      expectMsg(Reduce(6, 0, 3, 0))
      println("finishing the reduce part")
      //worker ! Reduce(12, 0, 0, 1)

      worker ! Reduce(9, 3, 0, 0)
      expectMsg(CompleteAllreduce(0, 0))
      worker ! Reduce(11, 1, 0, 1)
      worker ! Reduce(10, 2, 0, 1)
      worker ! Reduce(9, 3, 0, 1)
      expectMsg(CompleteAllreduce(0, 1))
    }
    "delayed future reduce" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker6")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 0.75
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start delayed future reduce test!==============")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0, 0, 0, 0))
      expectMsg(Scatter(1.0, 0, 1, 0))
      expectMsg(Scatter(2.0, 0, 2, 0))
      expectMsg(Scatter(3.0, 0, 3, 0))

      worker ! Scatter(2.0, 1, 0, 0)
      worker ! Scatter(4.0, 2, 0, 0)
      worker ! Scatter(6.0, 3, 0, 0)
      expectMsg(Reduce(12, 0, 0, 0))
      expectMsg(Reduce(12, 0, 1, 0))
      expectMsg(Reduce(12, 0, 2, 0))
      expectMsg(Reduce(12, 0, 3, 0))
      worker ! StartAllreduce(1) // master call it to do round 1
      worker ! Scatter(3.0, 1, 0, 1)
      worker ! Scatter(5.0, 2, 0, 1)
      worker ! Scatter(7.0, 3, 0, 1)
      // we send scatter value of round 1 to peers in case someone need it
      expectMsg(Scatter(1.0, 0, 0, 1))
      expectMsg(Scatter(2.0, 0, 1, 1))
      expectMsg(Scatter(3.0, 0, 2, 1))
      expectMsg(Scatter(4.0, 0, 3, 1))
      expectMsg(Reduce(15, 0, 0, 1))
      expectMsg(Reduce(15, 0, 1, 1))
      expectMsg(Reduce(15, 0, 2, 1))
      expectMsg(Reduce(15, 0, 3, 1))
      println("finishing the reduce part")
      // assertion: reduce t would never come after reduce t+1. (FIFO of message) otherwise would fail!
      worker ! Reduce(11, 1, 0, 0)
      worker ! Reduce(11, 1, 0, 1)
      worker ! Reduce(10, 2, 0, 0)
      worker ! Reduce(10, 2, 0, 1)
      worker ! Reduce(9, 3, 0, 0)
      worker ! Reduce(9, 3, 0, 1)
      expectMsg(CompleteAllreduce(0, 0))
      expectMsg(CompleteAllreduce(0, 1))
    }
    "simple catchup" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker7")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 1
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      println("===============start simple catchup test!==============")
      for (i <- 0 until 6) {
        worker ! StartAllreduce(i)
        expectMsg(Scatter(0 + i, 0, 0, i))
        expectMsg(Scatter(1 + i, 0, 1, i))
        expectMsg(Scatter(2 + i, 0, 2, i))
        expectMsg(Scatter(3 + i, 0, 3, i))
        worker ! Scatter(1.0, 1, 0, i)
        worker ! Scatter(2.0, 2, 0, i)
        worker ! Scatter(4.0, 3, 0, i)
        worker ! Reduce(12, 1, 0, i)
        worker ! Reduce(11, 2, 0, i)
        worker ! Reduce(10, 3, 0, i)
      }
      worker ! StartAllreduce(6) // trigger the first round to catchup
      expectMsg(Reduce(7.0, 0, 0, 0))
      expectMsg(Reduce(7.0, 0, 1, 0))
      expectMsg(Reduce(7.0, 0, 2, 0))
      expectMsg(Reduce(7.0, 0, 3, 0))
      expectMsg(CompleteAllreduce(0, 0))
      expectMsg(Scatter(0 + 6, 0, 0, 6))
      expectMsg(Scatter(1 + 6, 0, 1, 6))
      expectMsg(Scatter(2 + 6, 0, 2, 6))
      expectMsg(Scatter(3 + 6, 0, 3, 6))
      worker ! StartAllreduce(7) // test another round to make sure it works
      expectMsg(Reduce(7.0, 0, 0, 1))
      expectMsg(Reduce(7.0, 0, 1, 1))
      expectMsg(Reduce(7.0, 0, 2, 1))
      expectMsg(Reduce(7.0, 0, 3, 1))
      expectMsg(CompleteAllreduce(0, 1))
      expectMsg(Scatter(0 + 7, 0, 0, 7))
      expectMsg(Scatter(1 + 7, 0, 1, 7))
      expectMsg(Scatter(2 + 7, 0, 2, 7))
      expectMsg(Scatter(3 + 7, 0, 3, 7))
    }
    "cold catchup" in {
      // extreme case of catch up, only for test use
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker8")
      val workers: Map[Int, ActorRef] = HashMap(0 -> self, 1 -> self, 2 -> self, 3 -> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 1
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      worker ! StartAllreduce(10) // trigger the problem instantly
      // NOTE: here we need to nullify the reduce message instead.
      for (i <- 0 until 5) {
        expectMsg(Reduce(0, 0, 0, i))
        expectMsg(Reduce(0, 0, 1, i))
        expectMsg(Reduce(0, 0, 2, i))
        expectMsg(Reduce(0, 0, 3, i))
        expectMsg(CompleteAllreduce(0, i))

      }
      for (i <- 0 to 10) {
        expectMsg(Scatter(0 + i, 0, 0, i))
        expectMsg(Scatter(1 + i, 0, 1, i))
        expectMsg(Scatter(2 + i, 0, 2, i))
        expectMsg(Scatter(3 + i, 0, 3, i))
      }

    }
  }
}