package sample.cluster.allreduce.buffer

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class DataBufferSpec extends WordSpec with Matchers {


  "Reducing buffer behavior" should {

    val dataSize = 5
    val peerSize = 3
    val maxLag = 4
    val threshold = 0.7f
    val maxChunkSize = 2

    val rowAtTest = 1

    val buffer = DataBuffer(dataSize, peerSize, maxLag, threshold, maxChunkSize)

    "initialize buffers" in {

      buffer.temporalBuffer.length shouldEqual maxLag
      buffer.temporalBuffer(0).length shouldEqual peerSize
      buffer.temporalBuffer(0)(0).length shouldEqual dataSize

    }

    "have zero counts" in {

      for (r <- 0 until maxLag) {
        for (chunkId <- 0 until maxChunkSize) {
          buffer.count(r, chunkId) shouldEqual 0
        }
      }

    }

    "store first peer first chunk data" in {
      val srcId = 0
      val chunkId = 0
      val toStore: Array[Float] = randomFloatArray(maxChunkSize)
      buffer.store(toStore, rowAtTest, srcId, chunkId)
      buffer.count(rowAtTest, chunkId) shouldEqual 1

    }

    "store last peer last chunk with possibly small size" in {
      val srcId = peerSize - 1
      val chunkId = buffer.numChunks - 1
      intercept[ArrayIndexOutOfBoundsException] {
        val toStore: Array[Float] = randomFloatArray(maxChunkSize)
        buffer.store(toStore, rowAtTest, srcId, chunkId)
      }

      val lastChunkSize = dataSize - (buffer.numChunks - 1) * maxChunkSize
      val toStore: Array[Float] = randomFloatArray(lastChunkSize)
      buffer.store(toStore, rowAtTest, srcId, chunkId)
      buffer.count(rowAtTest, chunkId) shouldEqual 1
    }

    "store until reach threshold " in {

      buffer.reachRoundThreshold(rowAtTest) shouldBe false

      // 6 chunks of reduced data required
      // 0.7 * 3 * 3 - (threshold * peer size * num chunks)

      // peer 0, second chunk
      buffer.store(randomFloatArray(maxChunkSize), row = 1, srcId = 0, chunkId = 1)
      buffer.reachRoundThreshold(rowAtTest) shouldBe false

      // peer 1, first, second chunk
      buffer.store(randomFloatArray(maxChunkSize), row = 1, srcId = 1, chunkId = 0)
      buffer.store(randomFloatArray(maxChunkSize), row = 1, srcId = 1, chunkId = 1)
      buffer.reachRoundThreshold(rowAtTest) shouldBe false

      // peer 2, second chunk
      buffer.store(randomFloatArray(maxChunkSize), row = 1, srcId = 2, chunkId = 1)
      buffer.reachRoundThreshold(rowAtTest) shouldBe true

    }


    "get reduced row" in {
      // peer 0 - missing 3rd chunk
      // peer 1 - missing 3rd chunk
      // peer 2 - missing 1st chunk

      val reducedRowsInPeerOrder = buffer.get(rowAtTest)
      reducedRowsInPeerOrder(0)(4) shouldEqual 0
      reducedRowsInPeerOrder(1)(4) shouldEqual 0
      reducedRowsInPeerOrder(2)(0) shouldEqual 0
      reducedRowsInPeerOrder(2)(1) shouldEqual 0
    }

  }


  private def randomFloatArray(maxChunkSize: Int) = {
    Array.range(0, maxChunkSize).toList.map(_ => Random.nextFloat()).toArray
  }
}
