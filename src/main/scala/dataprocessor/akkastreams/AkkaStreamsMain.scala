package dataprocessor.akkastreams

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import dataprocessor.data.{BatchWork, SumProcessor}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AkkaStreamsProcessor {

  def getSource(batchWork: BatchWork): Source[Int, NotUsed] = {
    Source(batchWork.items)
      .flatMapConcat(item => Source(item.data))
  }

  def getProcessingFlow(processor: (Int, Int) => Int): Flow[Int, Int, NotUsed] = {
    Flow[Int].fold(0)(processor)
  }

  def process(batchWork: BatchWork)(using processor: (Int, Int) => Int, actorSystem: ActorSystem): Future[Int] = {
    getSource(batchWork)
      .via(getProcessingFlow(processor))
      .runWith(Sink.fold(0)(processor))
  }

}

@main def start(): Unit =
  given processor: Function2[Int, Int, Int] = (x1: Int, x2: Int) => x1 + x2

  val batchWork = BatchWork.generate(100, 1000)

  val expected = batchWork.items.flatMap(_.data).reduce(processor)
  println("Expected result: " + expected)

  given system: ActorSystem = ActorSystem("stream")
  given ec: ExecutionContext = system.dispatcher

  val result = AkkaStreamsProcessor.process(batchWork)

  result.onComplete {
    case Success(result) =>
      println("Result: " + result)
      system.terminate()
    case Failure(ex) =>
      println("Failed with " + ex)
      system.terminate()
  }