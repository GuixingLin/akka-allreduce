import sample.cluster.allreduce.AllreduceMaster
import sample.cluster.allreduce.ThresholdConfig
import sample.cluster.allreduce.DataConfig
import sample.cluster.allreduce.WorkerConfig


val threshold = ThresholdConfig(
  thAllreduce = 0.50f,
  thReduce = 0.50f,
  thComplete = 0.50f
)

val dataConfig = DataConfig(
  dataSize = 500000,
  maxChunkSize = 20000,
  maxRound = 100
)

val workerConfig = WorkerConfig(
  totalSize = 4,
  maxLag = 3
)

AllreduceMaster.startUp("2551", threshold, dataConfig, workerConfig)