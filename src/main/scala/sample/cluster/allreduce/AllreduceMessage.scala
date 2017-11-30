package sample.cluster.allreduce

import akka.actor.ActorRef


// worker messages
final case class InitWorkers(
	workers: Map[Int, ActorRef], 
	master : ActorRef,
	destId : Int, 
	thReduce : Double, 
	thComplete : Double,
	maxLag : Int,
	dataSize: Int
)
final case class StartAllreduce(round : Int)
final case class Scatter(value : Double, srcId : Int, destId : Int, round : Int)
final case class ScatterBlock(value : Array[Double], srcId : Int, destId : Int, round : Int)

final case class Reduce(value : Double, srcId : Int, destId : Int, round : Int)
final case class ReduceBlock(value: Array[Double], srcId : Int, destId : Int, round : Int)

final case class CompleteAllreduce(srcId : Int, round : Int)