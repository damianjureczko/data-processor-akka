package dataprocessor.akkastreams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import dataprocessor.data.{BatchItem, BatchWork}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AkkaStreamsProcessorTest extends BaseAkkaStreamsSpec {

  implicit val system: ActorSystem = ActorSystem("test-system")

  "AkkaStreamsProcessor" should {

    "create source from batch work" in {
      val batchWork = BatchWork(List(
        BatchItem(List(1, 2, 3)),
        BatchItem(List(4, 5, 6)),
        BatchItem(List(7, 8, 9))
      ))

      val itemsSource = AkkaStreamsProcessor.getSource(batchWork)

      itemsSource.runWith(TestSink[Int]())
        .request(9)
        .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9)
        .expectComplete()
    }

    "process data in flow" in {
      val (pub, sub) = TestSource[Int]()
        .via(AkkaStreamsProcessor.getProcessingFlow(_ + _))
        .toMat(TestSink[Int]())(Keep.both).run()

      sub.request(3)
      pub.sendNext(5)
      pub.sendNext(10)
      pub.sendNext(15)
      pub.sendComplete()
      sub.expectNext(30)
      sub.expectComplete()
    }

    "process batch work" in {
      val batchWork = BatchWork(List(
        BatchItem(List(1, 2, 3)),
        BatchItem(List(4, 5, 6)),
        BatchItem(List(7, 8, 9))
      ))

      given processor: Function2[Int, Int, Int] = _ + _

      val future = AkkaStreamsProcessor.process(batchWork)
      val result = Await.result(future, 3.seconds)

      result must be (45)
    }
  }
}
