/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.index

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionListener
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.GetChannelListRequest
import org.opensearch.commons.notifications.action.GetChannelListResponse
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.index.engine.Engine
import org.opensearch.index.shard.IndexingOperationListener
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.util.SecurityUtils

class TaskListener(
    val clusterService: ClusterService,
    val client: Client,
    var xContentRegistry: NamedXContentRegistry
) : IndexingOperationListener {

    val log: Logger = LogManager.getLogger(TaskListener::class.java)

    override fun postIndex(shardId: ShardId, index: Engine.Index, result: Engine.IndexResult) {
        if (result.resultType == Engine.Result.Type.FAILURE) {
            log.info(
                "Indexing failed for job {} on index {}",
                index.id(),
                shardId.indexName
            )
            return
        }

        if (client !is NodeClient) {
            log.error("client must be instance of NodeClient, it is actual as ${client.javaClass}")
            return
        }

        if (!shardId.indexName.equals(".tasks")) {
            log.debug("index ${shardId.indexName} is not [.tasks], ignore")
            return
        }

        val uuid = shardId.index.uuid

        val payload = index.source().utf8ToString()

        client.threadPool().threadContext.stashContext().use {
            // We need to set the user context information in the thread context for notification plugin to correctly resolve the user object
            client.threadPool().threadContext.putTransient(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                SecurityUtils.generateUserString(null)
            )
            client.threadPool().threadContext.stashContext().use {
                NotificationsPluginInterface.getChannelList(
                    client,
                    GetChannelListRequest(),
                    object : ActionListener<GetChannelListResponse> {
                        override fun onResponse(response: GetChannelListResponse) {
                            val channels = response.searchResult.objectList.map { it.configId }

                            NotificationsPluginInterface.sendNotification(
                                client,
                                EventSource("reindex", uuid, SeverityType.INFO),
                                ChannelMessage("reindex task has completed with result $payload", null, null),
                                channels,
                                object : ActionListener<SendNotificationResponse> {
                                    override fun onResponse(response: SendNotificationResponse) {
                                        log.info(
                                            "notification delivery status {}",
                                            response.notificationEvent.statusList.first().deliveryStatus
                                        )
                                    }

                                    override fun onFailure(e: java.lang.Exception) {
                                        log.error("notification delivery with error {}", e.message, e)
                                    }
                                }
                            )
                        }

                        override fun onFailure(e: Exception) {
                            log.error("get notification channels with error {}", e.message, e)
                        }
                    }
                )
            }
        }
    }
}
