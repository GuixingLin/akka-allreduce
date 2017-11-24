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

  "An allreduce worker" must {

    "single-round allreduce" in {
      val worker = system.actorOf(Props[AllreduceWorker], name = "worker")
      val workers : Map[Int, ActorRef] = HashMap(0->self, 1-> self, 2-> self, 3-> self)
      val idx = 0
      val thReduce = 1
      val thComplete = 0.75
      val maxLag = 5
      worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
      printf("start normal test!")
      worker ! StartAllreduce(0)
      expectMsg(Scatter(0.0,0,0,0))
      expectMsg(Scatter(1.0,0,1,0))
      expectMsg(Scatter(2.0,0,2,0))
      expectMsg(Scatter(3.0,0,3,0))
      worker ! Scatter(0.0,0,0,0)
      worker ! Scatter(2.0,1,0,0)
      worker ! Scatter(4.0,2,0,0)
      worker ! Scatter(6.0,3,0,0)
      expectMsg(Reduce(12,0,0,0))
      expectMsg(Reduce(12,0,1,0))
      expectMsg(Reduce(12,0,2,0))
      expectMsg(Reduce(12,0,3,0))
      worker ! Reduce(12,0,0,0)
      worker ! Reduce(11,1,0,0)
      worker ! Reduce(10,2,0,0)
      worker ! Reduce(9,3,0,0)
      expectMsg(CompleteAllreduce(0,0))
    }



  }
}