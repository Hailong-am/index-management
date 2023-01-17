/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.actionfilter

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.support.ActionFilter
import org.opensearch.action.support.ActionFilterChain
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.tasks.Task

private val logger = LogManager.getLogger(NotificationFilter::class.java)

class NotificationFilter(
    val clusterService: ClusterService,
    val settings: Settings
) : ActionFilter {

    override fun <Request : ActionRequest?, Response : ActionResponse?> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
        if (request is ResizeRequest || request is ReindexRequest) {
            logger.info("resize")
        }
        chain.proceed(task, action, request, listener)
    }

    /**
     * The FieldCapabilitiesResponse can contain merged or unmerged data. The response will hold unmerged data if its a cross cluster search.
     *
     * There is a boolean available in the FieldCapabilitiesRequest `isMergeResults` which indicates if the response is merged/unmerged.
     * Unfortunately this is package private and when rewriting we can't access it from request. Instead will be relying on the response.
     * If response has indexResponses then its unmerged else merged.
     */

    override fun order(): Int {
        return Integer.MAX_VALUE
    }
}
