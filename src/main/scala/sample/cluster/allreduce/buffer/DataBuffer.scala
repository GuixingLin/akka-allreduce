package sample.cluster.allreduce.buffer

case class DataBuffer(dataSize: Int,
                      peerSize: Int,
                      maxLag: Int,
                      threshold: Float,
                      maxChunkSize: Int) { 

  type Buffer = Array[Array[Float]]
  var temporalOffset = 0

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

  private val countFilled: Array[Array[Int]] = Array.ofDim[Int](maxLag, numChunks)
  private val minRequired: Int = (threshold * peerSize).toInt
  private val minChunksRequired: Int = (threshold * peerSize * numChunks).toInt

  def reachThreshold(row: Int, chunkId: Int): Boolean = {
    countFilled((row+temporalOffset)%maxLag)(chunkId) == minRequired
  }

  def store(data: Array[Float], row: Int, srcId: Int, chunkId: Int) = {
    val array = temporalBuffer((row+temporalOffset)%maxLag)(srcId)
    System.arraycopy(
      data, 0,
      array, chunkId * maxChunkSize,
      data.size)

    countFilled((row+temporalOffset)%maxLag)(chunkId) += 1
  }

  def count(row: Int, chunkId: Int): Int = {
    countFilled((row+temporalOffset)%maxLag)(chunkId)
  }

  def get(row: Int): Buffer = {
    temporalBuffer((row+temporalOffset)%maxLag)
  }

  def get(row: Int, chunkId: Int): (Buffer, Int) = {
    val endPos = math.min(dataSize, (chunkId + 1) * maxChunkSize)
    val length = endPos - chunkId * maxChunkSize
    var output: Array[Array[Float]] = Array.empty
    for (i <- 0 until temporalBuffer(row).length){
      output :+= temporalBuffer((row+temporalOffset)%maxLag)(i).slice(chunkId * maxChunkSize, endPos)
    }
    (output, length)
  }

  def up(): Unit = {
    temporalOffset = (temporalOffset + 1) % maxLag
    temporalBuffer((temporalOffset + maxLag - 1)%maxLag) = initializePeerBuffer()
    countFilled((temporalOffset + maxLag - 1)%maxLag) = Array.fill(numChunks)(0);
  }

  def reachRoundThreshold(row: Int) : Boolean = {
    var chunksCompleteReduce = 0
    for (i <- 0 until countFilled(row).length){
        chunksCompleteReduce += countFilled((row+temporalOffset)%maxLag)(i);
    }
    chunksCompleteReduce == minChunksRequired
  }

}

object DataBuffer {
  def empty = {
    DataBuffer(0, 0, 0, 0f, 1024)
  }
}