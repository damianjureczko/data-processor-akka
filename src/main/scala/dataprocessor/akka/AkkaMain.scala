package dataprocessor.akka

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import dataprocessor.akka.AkkaProcessor.ProcessBatchWork
import dataprocessor.data.{BatchItem, BatchWork, DataProcessor, SumProcessor}

object Main {
  case class BatchWorkResult(result: Int)

  def apply(batchWork: BatchWork)(using DataProcessor): Behavior[BatchWorkResult] =
    Behaviors.setup { context =>
      val processor = context.spawn(AkkaProcessor(5), "batch-processor")
      processor ! ProcessBatchWork(batchWork, context.self)

      Behaviors.receiveMessage { message =>
        context.log.info("RESULT ==========> {} <========== RESULT", message.result)
        Behaviors.stopped
      }
    }
}

@main def start(): Unit =
  val batchWork = BatchWork.generate(100, 1000)
  val expected = SumProcessor.process(batchWork.items.flatMap(_.data))
  println("Expected result: " + expected)

  ActorSystem(Main(batchWork)(using SumProcessor), "main")
