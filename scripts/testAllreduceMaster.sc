import sample.cluster.allreduce.AllreduceMaster
import sample.cluster.allreduce.ThresholdConfig
import sample.cluster.allreduce.DataConfig
import sample.cluster.allreduce.WorkerConfig


val threshold = ThresholdConfig(
  thAllreduce = 1f,
  thReduce = 1f,
  thComplete = 1f
)

val dataConfig = DataConfig(
  dataSize = 778,
  maxChunkSize = 3,
  maxRound = 1000
)

val workerConfig = WorkerConfig(
  totalSize = 4,
  maxLag = 3
)

AllreduceMaster.startUp("2551", threshold, dataConfig, workerConfig)