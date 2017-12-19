package sample.cluster.allreduce.buffer

import java.util

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
  private val countReduceFilled: Array[Array[Int]] = Array.ofDim[Int](maxLag, peerSize * numChunks)

  private val minRequired: Int = (threshold * peerSize).toInt
  private val minChunksRequired: Int = (threshold * peerSize * numChunks).toInt

  def reachThreshold(row: Int, chunkId: Int): Boolean = {
    countFilled(timeIdx(row))(chunkId) == minRequired
  }

  def storeWithCount(data: Array[Float], row: Int, srcId: Int, chunkId: Int, count: Int) = {
    store(data, row, srcId, chunkId)
    countReduceFilled(timeIdx(row))(srcId * numChunks + chunkId) = count
  }


  def store(data: Array[Float], row: Int, srcId: Int, chunkId: Int) = {
    val array = temporalBuffer(timeIdx(row))(srcId)
    System.arraycopy(
      data, 0,
      array, chunkId * maxChunkSize,
      data.size)
    countFilled(timeIdx(row))(chunkId) += 1
  }

  def count(row: Int, chunkId: Int): Int = {
    countFilled(timeIdx(row))(chunkId)
  }

  def getAll(row: Int, totalSize: Int): (Array[Float], Array[Int]) = {
    val output = temporalBuffer(timeIdx(row))
    val countOverPeerChunks = countReduceFilled(timeIdx(row))

    val dataOutput = Array.fill[Float](totalSize)(0.0f)
    val countOutput = Array.fill[Int](totalSize)(0)
    var transferred = 0
    var countTransferred = 0

    for (i <- 0 until peerSize) {
      val chunk = output(i)
      val chunkSize = Math.min(totalSize - transferred, chunk.size)
      System.arraycopy(chunk, 0, dataOutput, transferred, chunkSize)
      for (j <- 0 until numChunks) {
        val countChunkSize = {
          val tmp = Math.min(maxChunkSize, dataSize - maxChunkSize * j)
          Math.min(totalSize - countTransferred, tmp)
        }
        util.Arrays.fill(countOutput, countTransferred, countTransferred + countChunkSize, countOverPeerChunks(i * numChunks + j))
        countTransferred += countChunkSize
      }
      transferred += chunkSize
    }

    (dataOutput, countOutput)
  }

  private def timeIdx(row: Int) = {
    (row + temporalOffset) % maxLag
  }

  def get(row: Int, chunkId: Int): (Buffer, Int) = {
    val endPos = math.min(dataSize, (chunkId + 1) * maxChunkSize)
    val length = endPos - chunkId * maxChunkSize
    var output: Array[Array[Float]] = Array.empty
    for (i <- 0 until temporalBuffer(row).length){
      output :+= temporalBuffer(timeIdx(row))(i).slice(chunkId * maxChunkSize, endPos)
    }
    (output, length)
  }

  def up(): Unit = {
    temporalOffset = (temporalOffset + 1) % maxLag
    temporalBuffer(timeIdx(maxLag - 1)) = initializePeerBuffer()
    countFilled(timeIdx(maxLag - 1)) = Array.fill(numChunks)(0);
    countReduceFilled(timeIdx(maxLag - 1)) = Array.fill(peerSize * numChunks)(0);
  }

  def reachRoundThreshold(row: Int) : Boolean = {
    var chunksCompleteReduce = 0
    for (i <- 0 until countFilled(row).length){
        chunksCompleteReduce += countFilled(timeIdx(row))(i);
    }
    chunksCompleteReduce == minChunksRequired
  }

}

object DataBuffer {
  def empty = {
    DataBuffer(0, 0, 0, 0f, 1024)
  }
}