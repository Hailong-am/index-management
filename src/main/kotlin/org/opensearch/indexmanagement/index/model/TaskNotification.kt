/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.index.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.authuser.User
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import java.time.Instant
import java.time.temporal.ChronoUnit

data class TaskNotification(
    val channel: Channel,
    val condition: Condition,
    val operation: Operation,
    val jobSchedule: Schedule,
    val user: User? = null,
    val jobEnabled: Boolean,
    val jobEnabledTime: Instant?,
    val jobLastUpdateTime: Instant,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
) : ScheduledJobParameter, Writeable {

    override fun toXContent(builder: XContentBuilder, param: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(CHANNEL, channel)
            .field(CONDITIONS, condition)
            .field(OPERATION, operation)
            .field(JOB_SCHEDULE, jobSchedule)
            .field(USER, user)
            .field(ENABLED_FIELD, jobEnabled)
            .field(ENABLED_TIME_FIELD, jobEnabledTime)
            .field(LAST_UPDATED_TIME_FIELD, jobLastUpdateTime)
            .endObject()
    }

    override fun getName() = "TaskNotification"

    override fun getLastUpdateTime() = jobLastUpdateTime

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun isEnabled() = jobEnabled

    override fun writeTo(out: StreamOutput) {
        channel.writeTo(out)
        condition.writeTo(out)
        operation.writeTo(out)
        jobSchedule.writeTo(out)
        out.writeBoolean(jobEnabled)
        out.writeInstant(jobEnabledTime)
        out.writeInstant(jobLastUpdateTime)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        user?.writeTo(out)
    }

    companion object {
        const val TASK_NOTIFICATION_TYPE = "task_notification"
        const val CHANNEL = "channel"
        const val OPERATION = "operation"
        const val CONDITIONS = "conditions"
        const val JOB_SCHEDULE = "jobSchedule"
        const val USER = "user"
        const val ENABLED_FIELD = "enabled"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"

        fun parse(
            xcp: XContentParser,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): TaskNotification {
            var channel: Channel? = null
            var operation: Operation? = null
            var condition: Condition? = null
            val jobSchedule = IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES)
            var user: User? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    CHANNEL -> channel = Channel.parse(xcp)
                    OPERATION -> operation = Operation.parse(xcp)
                    CONDITIONS -> condition = Condition.parse(xcp)
                    USER -> user = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in conditions.")
                }
            }

            return TaskNotification(
                requireNotNull(channel) { "$CHANNEL field must not be null" },
                requireNotNull(condition) { "$CONDITIONS field must not be null" },
                requireNotNull(operation) { "$OPERATION field must not be null" },
                jobSchedule,
                user,
                true,
                Instant.now(),
                Instant.now(),
                seqNo,
                primaryTerm
            )
        }
    }

    data class Operation(
        val name: String,
        val source: String,
        val target: String,
        val taskId: String
    ) : ToXContentObject, Writeable {
        override fun toXContent(builder: XContentBuilder, param: ToXContent.Params): XContentBuilder {
            return builder.startObject().field(NAME, name).field(SOURCE, source).field(TARGET, target)
                .field(TASK_ID, taskId).endObject()
        }

        constructor(sin: StreamInput) : this(
            name = sin.readString(),
            source = sin.readString(),
            target = sin.readString(),
            taskId = sin.readString()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeString(name)
            out.writeString(source)
            out.writeString(target)
            out.writeString(taskId)
        }

        companion object {
            const val NAME = "name"
            const val SOURCE = "source"
            const val TARGET = "target"
            const val TASK_ID = "task_id"

            fun parse(xcp: XContentParser): Operation {
                var name = ""
                var source = ""
                var target = ""
                var taskId = ""

                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        NAME -> name = xcp.text()
                        SOURCE -> source = xcp.text()
                        TASK_ID -> taskId = xcp.text()
                        TARGET -> target = xcp.text()
                        else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in operation.")
                    }
                }

                return Operation(name, source, target, taskId)
            }
        }
    }

    data class Condition(
        val complete: Boolean = false,
        val failure: Boolean = false
    ) : ToXContentObject, Writeable {
        override fun toXContent(builder: XContentBuilder, param: ToXContent.Params): XContentBuilder {
            return builder.startObject().field(COMPLETE, complete).field(FAILURE, failure)
        }

        override fun writeTo(out: StreamOutput) {
            out.writeBoolean(complete)
            out.writeBoolean(failure)
        }

        companion object {
            const val COMPLETE = "complete"
            const val FAILURE = "failure"

            fun parse(xcp: XContentParser): Condition {
                var complete = false
                var failure = false

                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        COMPLETE -> complete = xcp.booleanValue()
                        FAILURE -> failure = xcp.booleanValue()
                        else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in conditions.")
                    }
                }

                return Condition(complete, failure)
            }
        }
    }
}
