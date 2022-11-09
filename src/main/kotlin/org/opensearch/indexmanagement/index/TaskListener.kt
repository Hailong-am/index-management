/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.index

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.engine.Engine
import org.opensearch.index.shard.IndexingOperationListener
import org.opensearch.index.shard.ShardId

class TaskListener(
    var clusterService: ClusterService
) : IndexingOperationListener {

    val log: Logger = LogManager.getLogger(TaskListener::class.java)

    override fun postIndex(shardId: ShardId, index: Engine.Index, result: Engine.IndexResult) {
        if (result.resultType == Engine.Result.Type.FAILURE) {
            log.info(
                "Indexing failed for job {} on index {}", index.id(), shardId.indexName
            )
            return
        }

        val localNodeId: String = clusterService.localNode().id
        val routingTable: IndexShardRoutingTable = clusterService.state().routingTable().shardRoutingTable(shardId)
        val shardNodeIds: MutableList<String> = ArrayList()
        for (shardRouting in routingTable) {
            if (shardRouting.active()) {
                shardNodeIds.add(shardRouting.currentNodeId())
            }
        }
    }
}
