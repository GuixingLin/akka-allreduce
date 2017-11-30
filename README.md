Code lies in "akka/src/main/cluster/sample/cluster/allreduce/"

Run Master: sbt "runMain sample.cluster.Allreduce.AllreduceMaster"

Run Worker: sbt "runMain sample.cluster.Allreduce.AllreduceWorker $PORT"

It assumes cluster configuration at resources/application.conf All-reduce configurations are now in AllreduceMaster. They include worker size, maximal lag, and different completion thresholds.