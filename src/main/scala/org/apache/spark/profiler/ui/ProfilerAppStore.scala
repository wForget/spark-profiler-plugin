package org.apache.spark.profiler.ui

import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.AppStatusStore

case class ProfilerAppStore(store: AppStatusStore) {

  val sqlStatusStore = new SQLAppStatusStore(store.store)

  def listExecutions(): Seq[String] = {
    store.executorList(activeOnly = false).map(_.id)
  }

  def listJobs(): Seq[Int] = {
    store.jobsList(null).map(_.jobId)
  }

  def listStages(): Seq[(Int, Int)]  = {
    store.stageList(null).map(s => (s.stageId, s.attemptId))
  }

  def listTask(stageId: Int, stageAttemptId: Int): Seq[Long] = {
    store.taskList(stageId, stageAttemptId, 1000).map(_.taskId)
  }

}
