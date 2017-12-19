package sample.cluster.allreduce.buffer

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class ScatteredDataBufferSpec extends WordSpec with Matchers {


  "Scattered buffer behavior" should {

    val dataSize = 5
    val peerSize = 4
    val maxLag = 4
    val reducingThreshold = 0.75f
    val maxChunkSize = 3
    val rowAtTest = 1

    val buffer = ScatteredDataBuffer(dataSize, peerSize, maxLag, reducingThreshold, maxChunkSize)
    val numChunks = buffer.numChunks
    val expectedCriticalPeerSize = 3


    "initialize buffers" in {

      buffer.temporalBuffer.length shouldEqual maxLag
      buffer.temporalBuffer(0).length shouldEqual peerSize
      buffer.temporalBuffer(0)(0).length shouldEqual dataSize

    }

    "throw exception when data to store at the end exceeds expected size" in {

      val lastChunkId = numChunks - 1
      intercept[ArrayIndexOutOfBoundsException] {
        val toStore: Array[Float] = randomFloatArray(maxChunkSize)
        buffer.store(toStore, rowAtTest, 0, lastChunkId)
      }
      val excess = numChunks * maxChunkSize - dataSize
      val toStore = randomFloatArray(maxChunkSize - excess)
      buffer.store(toStore, rowAtTest, 0, lastChunkId)
    }

    "reach reducing threshold" in {

      val reachingThresholdChunkId = 0
      val reachThreshold = List(false, false, true)
      for (i <- 0 until expectedCriticalPeerSize) {
        val toStore = randomFloatArray(maxChunkSize)
        buffer.store(toStore, rowAtTest, srcId = i, reachingThresholdChunkId)
        buffer.reachReducingThreshold(rowAtTest, reachingThresholdChunkId) shouldBe reachThreshold(i)
      }

    }

    "reduce values with correct count" in {
      val (emptyReduced, emptyCount) = buffer.reduce(0, 0)
      emptyCount shouldEqual 0
      emptyReduced.sum shouldEqual 0

      val (_, counts) = buffer.reduce(rowAtTest, 0)
      counts shouldEqual expectedCriticalPeerSize

    }

  }

  private def randomFloatArray(maxChunkSize: Int) = {
    Array.range(0, maxChunkSize).toList.map(_ => Random.nextFloat()).toArray
  }
}
