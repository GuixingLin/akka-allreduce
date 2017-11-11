package sample.cluster.allreduce

import akka.actor.ActorRef


// worker messages
final case class Neighbors(workers: collection.mutable.Map[Int, ActorRef], destId : Int)
final case class StartAllreduce()
final case class Scatter(value : Double, srcId : Int, destId : Int)
final case class Gather(value : Double, srcId : Int, destId : Int)