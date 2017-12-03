Code lies in "src/main/cluster/sample/cluster/allreduce/"

Run Master: sbt "runMain sample.cluster.allreduce.AllreduceMaster $PORT=2551 $numOfWorkers=2 $dataSize(msgSize)=10 $maxChunkSize=2"

Reduce Threshold, Scatter Threshold and Online Nodes Threshold are all hardcoded in AllreduceMaster. The configuration will be moved to application.conf soon.

Run Worker: sbt "runMain sample.cluster.Allreduce.AllreduceWorker $PORT $dataSize(msgSize)=10"

Be aware that these two "dataSize" in the commands must agree with each other. The dataSize variable means the size of the message sent retrieved from the caller. 

The loglevel is currently set to "INFO" which is specified in application.conf. Might switch to SLF4J in the future (as it requires some jar dependencies)

It assumes cluster configuration at resources/application.conf All-reduce configurations are now in AllreduceMaster. They include worker size, maximal lag, and different completion thresholds.