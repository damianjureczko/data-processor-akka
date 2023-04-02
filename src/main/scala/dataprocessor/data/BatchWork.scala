package dataprocessor.data

import scala.util.Random

case class BatchItem(data: List[Int])

case class BatchWork(items: List[BatchItem])

object BatchWork {
  private val rand = new Random()

  def generate(itemsNo: Int, dataSize: Int): BatchWork = {
    BatchWork((0 until itemsNo).toList.map(i => {
      BatchItem((0 until dataSize).toList.map(_ => generateData))
    }))
  }

  private def generateData: Int = rand.nextInt(100)
}

trait DataProcessor:
  def process(data: List[Int]): Int

object SumProcessor extends DataProcessor {
  override def process(data: List[Int]): Int = data.sum
}
