package sample.cluster.allreduce

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, RootActorPath, Terminated}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class AllreduceMaster(
  totalWorkers : Int,
  thAllreduce : Float,
  thReduce : Float, 
  thComplete : Float,
  maxLag : Int,
  dataSize: Int,
  maxRound: Int,
  maxChunkSize: Int
) extends Actor {

  var workers = Map[Int, ActorRef]()
  val cluster = Cluster(context.system)

  var round = -1
  var numComplete = 0

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {

    case MemberUp(m) =>
      println(s"----detect member ${m.address} up")
      register(m).onSuccess {
        case Done =>
          if (workers.size >= (totalWorkers * thAllreduce).toInt) {
            println(s"----${workers.size} (out of ${totalWorkers}) workers are up")
            init_workers()
            round = 0
            startAllreduce()
          }
      }

    case Terminated(a) =>
      println(s"$a is terminated, removing it from the set")
      for ((idx, worker) <- workers){
        if(worker == a) {
          workers -= idx
        }
      }

    case c : CompleteAllreduce =>
      println(s"----node ${c.srcId} completes allreduce round ${c.round}")
      if (c.round == round) {
        numComplete += 1
        if (numComplete >= totalWorkers * thAllreduce && round < maxRound) {
          println(s"----${numComplete} (out of ${totalWorkers}) workers complete round ${round}\n")
          round += 1
          startAllreduce()
        }
      }
  }

  private def register(member: Member): Future[Done] =
    if (member.hasRole("worker")) {
      implicit val timeout = Timeout(5.seconds)
      context.actorSelection(RootActorPath(member.address) / "user" / "worker").resolveOne().map { workerRef =>
          context watch workerRef
          val new_idx: Integer = workers.size
          workers = workers.updated(new_idx, workerRef)
          println(s"----current size = ${workers.size}")
          Done
      }
    } else {
      Future.successful(Done)
    }

    private def init_workers() = {
      for ((idx, worker) <- workers) {
        println(s"----init worker $idx $worker")
        worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag, dataSize, maxChunkSize)
      }
    }

    private def startAllreduce() = {
      println(s"----start allreduce round ${round}")
      numComplete = 0
      for ((idx, worker) <- workers) {
        worker ! StartAllreduce(round)
      }
    }
}


object AllreduceMaster {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val totalWorkers = 2
    val thAllreduce = 1f
    val thReduce = 0.9f
    val thComplete = 0.8f
    val maxLag = 1
    val maxRound = 100
    val dataSize = if (args.length <= 1) totalWorkers * 5 else args(1).toInt
    val port = if (args.isEmpty) "2551" else args(0)
    val maxChunkSize = if (args.length <= 2) 1 else args(2).toInt

    //debug
    println(s"----dataSize is :${dataSize}")

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val master = system.actorOf(
      Props(
        classOf[AllreduceMaster], 
        totalWorkers, 
        thAllreduce,
        thReduce, 
        thComplete, 
        maxLag,
        dataSize,
        maxRound,
        maxChunkSize
      ), 
      name = "master"
    )
  }
}