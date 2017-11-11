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

class AllreduceMaster(totalWorkers : Int) extends Actor {

  var workers: collection.mutable.Map[Int, ActorRef] = collection.mutable.Map[Int, ActorRef]()

  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {

    case MemberUp(m) =>
      println(s"----detect member ${m.address} up")
      register(m).onSuccess {
        case Done =>
          if (workers.size == totalWorkers) {
            println(s"----all workers are up, init workers, then start allreduce")
            init_workers()
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
          workers.update(new_idx, workerRef)
          println(s"----current size = ${workers.size}")
          Done
      }
    } else {
      Future.successful(Done)
    }

    private def init_workers() = {
      for ((idx, worker) <- workers) {
        worker ! Neighbors(workers, idx)
      }
    }

    private def startAllreduce() = {
      for ((idx, worker) <- workers) {
        worker ! StartAllreduce()
      }
    }
}


object AllreduceMaster {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val totalWorkers = 2
    val port = if (args.isEmpty) "2551" else args(0)

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    // val master = system.actorOf(Props(classOf[AllreduceMaster],layout), name = "master") //? what's second arg of props for
    val master = system.actorOf(Props(classOf[AllreduceMaster], totalWorkers), name = "master")
  }

  // def startUp(ports: List[String] = List("2551")): Unit = {
  //   ports foreach( eachPort => main(Array(eachPort)))
  // }
}