package dataprocessor.akka

import dataprocessor.akka.AkkaProcessor.BatchItemResult
import dataprocessor.akka.AkkaWorker.ProcessBatchItem
import dataprocessor.data.{BatchItem, DataProcessor}
import org.mockito.Mockito.when

class AkkaWorkerTest extends BaseAkkaSpec {

  "AkkaWorker" should {

    "return processed data" in {
      val dataProcessor = mock[DataProcessor]
      when(dataProcessor.process(List(1, 2, 3))).thenReturn(6)

      val worker = testKit.spawn(AkkaWorker()(using dataProcessor), "worker")
      val probe = testKit.createTestProbe[BatchItemResult]()

      worker ! ProcessBatchItem(100, BatchItem(List(1, 2, 3)), probe.ref)

      probe.expectMessage(BatchItemResult(100, 6, worker.ref))
    }
  }
}
