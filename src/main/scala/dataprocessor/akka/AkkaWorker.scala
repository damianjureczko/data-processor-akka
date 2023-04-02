package dataprocessor.akka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import dataprocessor.akka.AkkaProcessor.BatchItemResult
import dataprocessor.data.{BatchItem, DataProcessor}

object AkkaWorker {
  case class ProcessBatchItem(id: Int, batchItem: BatchItem, replyTo: ActorRef[BatchItemResult])

  def apply()(using processor: DataProcessor): Behavior[ProcessBatchItem] =
    Behaviors.receive { (context, message) =>
      val result = processor.process(message.batchItem.data)
      message.replyTo ! BatchItemResult(message.id, result, context.self)
      Behaviors.same
    }
}
