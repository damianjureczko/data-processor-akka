package dataprocessor.akka

import dataprocessor.akka.AkkaProcessor.{BatchItemResult, ProcessBatchWork}
import dataprocessor.akka.AkkaWorker.ProcessBatchItem
import dataprocessor.akka.Main.BatchWorkResult
import dataprocessor.data.{BatchItem, BatchWork, DataProcessor, SumProcessor}
import org.mockito.Mockito
import org.mockito.Mockito.{verify, when}

class AkkaProcessorTest extends BaseAkkaSpec {

  "AkkaProcessor" should {

    "return processed data" in {
      val dataProcessor = mock[DataProcessor]
      when(dataProcessor.process(List(1, 2, 3))).thenReturn(6)
      when(dataProcessor.process(List(6, 6, 6))).thenReturn(18)

      val processor = testKit.spawn(AkkaProcessor(workersNo = 2)(using dataProcessor), "processor")
      val probe = testKit.createTestProbe[BatchWorkResult]()

      val batchWork = BatchWork(List(
        BatchItem(List(1, 2, 3)),
        BatchItem(List(1, 2, 3)),
        BatchItem(List(1, 2, 3))
      ))

      processor ! ProcessBatchWork(batchWork, probe.ref)

      probe.expectMessage(BatchWorkResult(18))


    }
  }
}
