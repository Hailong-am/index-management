/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.action.index.IndexLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.index.IndexLRONConfigRequest
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestUpdateLRONConfigAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.PUT, "${IndexManagementPlugin.LRON_BASE_URI}/{id}")
        )
    }

    override fun getName(): String {
        return "update_lron_config_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val docId = request.param("id")
        val xcp = request.contentParser()
        val lronConfig = xcp.parseWithType(parse = LRONConfig.Companion::parse)
        if (getDocID(lronConfig.taskId, lronConfig.actionName) != docId) {
            throw IllegalArgumentException("docId isn't match with lron_config")
        }

        val indexLRONConfigRequest = IndexLRONConfigRequest(lronConfig, true)

        return RestChannelConsumer { channel ->
            client.execute(IndexLRONConfigAction.INSTANCE, indexLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
