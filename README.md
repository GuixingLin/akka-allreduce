Code lies in "akka/src/main/cluster/sample/cluster/allreduce/"

Run Master: sbt "runMain sample.cluster.AllReduce.AllReduceMaster"

Run Worker: sbt "runMain sample.cluster.AllReduce.AllReduceWorker $PORT"
