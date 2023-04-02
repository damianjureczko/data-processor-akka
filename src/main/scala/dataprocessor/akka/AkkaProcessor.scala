package dataprocessor.akka

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import dataprocessor.akka.AkkaProcessor.{BatchItemResult, ProcessorCommand}
import dataprocessor.akka.AkkaWorker.ProcessBatchItem
import dataprocessor.akka.Main.BatchWorkResult
import dataprocessor.data.{BatchItem, BatchWork, DataProcessor}

object AkkaProcessor {

  sealed trait ProcessorCommand

  final case class ProcessBatchWork(batchWork: BatchWork, replyTo: ActorRef[BatchWorkResult]) extends ProcessorCommand

  final case class BatchItemResult(id: Int, result: Int, workerRef: ActorRef[ProcessBatchItem]) extends ProcessorCommand

  private final case class Worker(workerRef: ActorRef[ProcessBatchItem], working: Boolean)

  private final case class WorkItem(id: Int, item: BatchItem, inProgress: Boolean, result: Option[Int]) {
    def isToDo: Boolean = !inProgress && result.isEmpty
  }

  def apply(workersNo: Int)(using processor: DataProcessor): Behavior[ProcessorCommand] =
    Behaviors.setup { context =>
      val ws = (0 until workersNo).toList.map { i =>
        val worker = context.spawn(AkkaWorker(), s"worker-$i")
        context.log.debug("Created worker: {}", worker)
        Worker(worker, working = false)
      }
      workersReady(ws)
    }

  private def workersReady(workers: List[Worker])(using processor: DataProcessor): Behavior[ProcessorCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case ProcessBatchWork(batchWork, replyTo) =>
          val items = createWorkItems(batchWork.items)

          val jobToDo = findJobToDo(workers, items)
          jobToDo.foreach { (worker, item) =>
            worker.workerRef ! ProcessBatchItem(item.id, item.item, context.self)
            context.log.debug("Sending [{}, {}] to {}", item.id, item.item, worker.workerRef)
          }

          val (workersToStart, itemsToStart) = jobToDo.unzip
          val newWorkers = startWorkers(workers, workersToStart.map(_.workerRef.path.name))
          val newItems = startItems(items, itemsToStart.map(_.id))

          working(newWorkers, newItems, replyTo)
        case _ =>
          context.log.warn("Unexpected message {}", message)
          Behaviors.same
      }
    }

  private def working(workers: List[Worker], items: List[WorkItem], replyTo: ActorRef[BatchWorkResult])(using processor: DataProcessor): Behavior[ProcessorCommand] =
    Behaviors.receive { (context, message) =>
      message match
        case BatchItemResult(id, result, workerRef) =>
          context.log.debug("Received result [{} - {}] from {}", id, result, workerRef)
          val updatedItems = saveResult(items, id, result)

          findItemToDo(updatedItems) match
            case Some(itemToDo) =>
              workerRef ! ProcessBatchItem(itemToDo.id, itemToDo.item, context.self)
              context.log.debug("Sending [{}, {}] to {}", itemToDo.id, itemToDo.item, workerRef)
              val finalItems = startItem(updatedItems, itemToDo.id)
              working(workers, finalItems, replyTo)
            case None =>
              val updatedWorkers = stopWorker(workers, workerRef.path.name)
              if updatedWorkers.exists(_.working == true) then
                context.log.debug("Waiting for working workers")
                working(updatedWorkers, updatedItems, replyTo)
              else
                val results = updatedItems.map(_.result).collect { case Some(result) => result }
                val result = processor.process(results)
                context.log.debug("Sending result: {}, from {}", result, results)
                replyTo ! BatchWorkResult(result)
                workersReady(updatedWorkers)
        case _ =>
          context.log.warn("Unexpected message {}", message)
          Behaviors.same
    }

  private def createWorkItems(items: List[BatchItem]): List[WorkItem] =
    items.zipWithIndex.map((item, idx) => WorkItem(idx, item, inProgress = false, result = None))

  private def findJobToDo(workers: List[Worker], items: List[WorkItem]): List[(Worker, WorkItem)] =
    val idleWorkers = workers.filter(!_.working)
    val todoItems = items.filter(i => !i.inProgress && i.result.isEmpty)
    idleWorkers.zip(todoItems)

  private def findItemToDo(items: List[WorkItem]): Option[WorkItem] = items.find(_.isToDo)

  private def startWorkers(workers: List[Worker], workersToStart: List[String]): List[Worker] =
    workers.map(worker => if workersToStart.contains(worker.workerRef.path.name) then worker.copy(working = true) else worker)

  private def stopWorker(workers: List[Worker], name: String): List[Worker] =
    workers.map(w => if w.workerRef.path.name == name then w.copy(working = false) else w)

  private def startItem(items: List[WorkItem], id: Int): List[WorkItem] =
    startItems(items, List(id))

  private def startItems(items: List[WorkItem], itemsToStart: List[Int]): List[WorkItem] =
    items.map(item => if itemsToStart.contains(item.id) then item.copy(inProgress = true) else item)

  private def saveResult(items: List[WorkItem], id: Int, result: Int): List[WorkItem] =
    items.map(item => if item.id == id then item.copy(inProgress = false, result = Some(result)) else item)

}

