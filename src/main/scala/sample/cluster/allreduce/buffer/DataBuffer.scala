package sample.cluster.allreduce.buffer

case class DataBuffer(dataSize: Int,
                      peerSize: Int,
                      maxLag: Int,
                      threshold: Double) {

  type Buffer = Array[Array[Double]]

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

  private var countFilled: Array[Int] = Array.fill(maxLag)(0)

  private val minRequired: Int = (threshold * peerSize).toInt

  def reachThreshold(row: Int): Boolean = {
    countFilled(row) == minRequired
  }

  def store(data: Array[Double], row: Int, srcId: Int, pos: Int = 0) = {

    val array = temporalBuffer(row)(srcId)

    System.arraycopy(
      data, 0,
      array, pos,
      data.size)

    countFilled(row) += 1
  }

  def count(row: Int): Int = {
    countFilled(row)
  }

  def get(row: Int): Buffer = {
    temporalBuffer(row)
  }

  def up(): Unit = {
    for (i <- 1 until maxLag) {
      temporalBuffer(i - 1) = temporalBuffer(i)
      countFilled(i - 1) = countFilled(i)
    }
    temporalBuffer(maxLag - 1) = initializePeerBuffer()
    countFilled(maxLag - 1) = 0
  }

}

object DataBuffer {
  def empty = {
    DataBuffer(0, 0, 0, 0)
  }
}