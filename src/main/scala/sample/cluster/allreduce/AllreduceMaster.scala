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
  thAllreduce : Double,
  thReduce : Double, 
  thComplete : Double,
  maxLag : Int,
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
          if (workers.size >= totalWorkers * thAllreduce) {
            println(s"----${workers.size} (out of ${totalWorkers}) workers are up")
            init_workers()
            round = 0
            startAllreduce()
          }
      }
      // println(s"----current size = ${workers.size}???") //? why this msg comes before println in register?

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
        if (numComplete >= totalWorkers * thAllreduce && round < 5) {
          println(s"----${numComplete} (out of ${totalWorkers}) workers complete round ${round}\n")
          round += 1
          startAllreduce()
        }
      }
  }

  // def setupLayout(): Unit =
  // // Organize grid
  // for((idx, worker) <- workers){
  //   val groups = layout.groups(idx)
  //   for (group <- groups){
  //     val member_idxs = layout.members(group)
  //     var members = Set[ActorRef]()
  //     for(member_id <- member_idxs) members+=workers(member_id)
  //     val addresses = GridGroupAddresses(group, members)
  //     println(s"To worker $idx $worker: Sending group address: $addresses")
  //     worker ! addresses
  //   }
  // }

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
        worker ! InitWorkers(workers, self, idx, thReduce, thComplete, maxLag)
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
    val totalWorkers = 5
    val thAllreduce = 0.5
    val thReduce = 0.9
    val thComplete = 0.8
    val maxLag = 1
    val port = if (args.isEmpty) "2551" else args(0)

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    // val master = system.actorOf(Props(classOf[AllreduceMaster],layout), name = "master") //? what's second arg of props for
    val master = system.actorOf(
      Props(
        classOf[AllreduceMaster], 
        totalWorkers, 
        thAllreduce,
        thReduce, 
        thComplete, 
        maxLag
      ), 
      name = "master"
    )
  }

  // def startUp(ports: List[String] = List("2551")): Unit = {
  //   ports foreach( eachPort => main(Array(eachPort)))
  // }
}