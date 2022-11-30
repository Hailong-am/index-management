/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.index

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.index.model.TaskNotification
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

object TaskNotificationRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("task_notification")) {

    private val log = LogManager.getLogger(javaClass)

    private lateinit var client: Client
    private lateinit var indicesManager: IndexManagementIndices
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var settings: Settings

    fun init(
        client: Client,
        threadPool: ThreadPool,
        settings: Settings,
        indicesManager: IndexManagementIndices,
        clusterService: ClusterService
    ): TaskNotificationRunner {
        this.client = client
        this.threadPool = threadPool
        this.settings = settings
        this.indicesManager = indicesManager
        this.clusterService = clusterService
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is TaskNotification) {
            return
        }

        val operationName = job.operation.name
        if (operationName == "reindex") {
            val taskId = job.operation.taskId
            val taskReq = GetTaskRequest()
            taskReq.taskId = TaskId(taskId)
            client.admin().cluster().getTask(
                taskReq,
                object : ActionListener<GetTaskResponse> {
                    override fun onResponse(response: GetTaskResponse) {
                        if (!response.task.isCompleted) {
                            log.info("Task ${response.task.task.description} is still running ")
                            return
                        }

                        val failed = response.task.error != null
                        log.info("Task has error ${response.task.errorAsMap}")
                        var message = "Reindex from ${job.operation.source} to ${job.operation.target} has completed"
                        if (failed) {
                            message += "with failure"
                        }

                        launch {
                            log.info("send notification $message")
                            Channel(job.channel.id).sendNotification(
                                client,
                                EventSource(operationName, context.jobId, SeverityType.INFO),
                                message,
                                job.user
                            )
                            // delete job document INDEX_MANAGEMENT_INDEX
                            client.delete(
                                DeleteRequest(INDEX_MANAGEMENT_INDEX, context.jobId),
                                object : ActionListener<DeleteResponse> {
                                    override fun onResponse(response: DeleteResponse) {
                                        log.error("Cleanup task notification for ${context.jobId} done")
                                    }

                                    override fun onFailure(e: Exception) {
                                        log.error("Cleanup task notification for ${context.jobId} with error", e)
                                    }
                                }
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        // ignore
                    }
                }
            )
        } else if (operationName == "split" || operationName == "shrink") {
            //
        }
    }
}
