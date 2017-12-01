package sample.cluster.allreduce.buffer

case class DataBuffer(dataSize: Int,
                      peerSize: Int,
                      maxLag: Int,
                      threshold: Float,
                      maxChunkSize: Int) { 

  //maxChunkSize is the maximum size of the msg that is allowed on the wire

  type Buffer = Array[Array[Float]]

  val numChunks =  math.ceil(1f * dataSize / maxChunkSize).toInt

  var temporalBuffer: Array[Buffer] = {
    Array.fill(maxLag) {
      initializePeerBuffer()
    }
  }

  private def initializePeerBuffer(): Buffer = {
    Array.fill(peerSize) {
      Array.fill(dataSize)(0)
    }
  }

  private var countFilled: Array[Array[Int]] = Array.ofDim[Int](maxLag, numChunks)

  private val minRequired: Int = (threshold * peerSize).toInt
  private val minChunksRequired: Int = (threshold * peerSize * numChunks).toInt

  def reachThreshold(row: Int, chunkId: Int): Boolean = {
    countFilled(row)(chunkId) == minRequired
  }

  def store(data: Array[Float], row: Int, srcId: Int, chunkId: Int, pos: Int = 0) = {
    val array = temporalBuffer(row)(srcId)
    //debug
    //println(s"---Array length: ${array.length}; Data length: ${data.length}")
    System.arraycopy(
      data, 0,
      array, chunkId * maxChunkSize,
      data.size)

    countFilled(row)(chunkId) += 1
  }

  def count(row: Int, chunkId: Int): Int = {
    countFilled(row)(chunkId)
  }

  def get(row: Int, chunkId: Int): (Buffer, Int) = {
    //temporalBuffer(row)
    var endPos = math.min(dataSize, (chunkId + 1) * maxChunkSize)
    var length = endPos - chunkId * maxChunkSize
    var output: Array[Array[Float]] = Array.empty
    for (i <- 0 until temporalBuffer(row).length){
      output :+= temporalBuffer(row)(i).slice(chunkId * maxChunkSize, endPos)
    }
    (output, length)
  }

  def up(): Unit = {
    for (i <- 1 until maxLag) {
      temporalBuffer(i - 1) = temporalBuffer(i)
      countFilled(i - 1) = countFilled(i)
    }
    temporalBuffer(maxLag - 1) = initializePeerBuffer()
    countFilled(maxLag - 1) = Array.fill(numChunks)(0);
  }

  def reachRoundThreshold(row: Int) : Boolean = {
    var chunksCompleteReduce = 0
    for (i <- 0 until countFilled(row).length){
        chunksCompleteReduce += countFilled(row)(i);
    }
    //debug
    //println(s"-----Have gathered ${chunksCompleteReduce} reduced chunks from peers")
    chunksCompleteReduce == minChunksRequired
  }

}

object DataBuffer {
  def empty = {
    DataBuffer(0, 0, 0, 0f, 1024)
  }
}