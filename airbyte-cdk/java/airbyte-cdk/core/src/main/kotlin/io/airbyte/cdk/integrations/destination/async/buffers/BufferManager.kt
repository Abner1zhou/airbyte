/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.destination.async.buffers

import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.cdk.integrations.destination.async.AirbyteFileUtils
import io.airbyte.cdk.integrations.destination.async.GlobalMemoryManager
import io.airbyte.cdk.integrations.destination.async.state.GlobalAsyncStateManager
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.lang.String.join

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(
    property = ConnectorConfigurationPropertySource.CONNECTOR_OPERATION,
    value = "write",
)
@Requires(env = ["destination"])
class BufferManager(
    private val globalMemoryManager: GlobalMemoryManager,
    private val globalAsyncStateManager: GlobalAsyncStateManager,
    private val asyncBuffers: AsyncBuffers,
    private val airbyteFileUtils: AirbyteFileUtils,
) {
    @Scheduled(initialDelay = "0s", fixedRate = "60s")
    fun printQueueInfo() {
        val queueInfo = StringBuilder().append("[ASYNC QUEUE INFO] ")
        val messages = mutableListOf<String>()

        messages.add(
            "Global: max: ${airbyteFileUtils.byteCountToDisplaySize(
                globalMemoryManager.maxMemoryBytes,
            )}, allocated: ${airbyteFileUtils.byteCountToDisplaySize(
                globalMemoryManager.currentMemoryBytes.get(),
            )} (${globalMemoryManager.currentMemoryBytes.get().toDouble() / 1024 / 1024} MB), %% used: ${globalMemoryManager.currentMemoryBytes.get().toDouble() / globalMemoryManager.maxMemoryBytes}",
        )

        for ((key, queue) in asyncBuffers.buffers.entries) {
            messages.add(
                "Queue `${key.name}`, num records: ${queue.size()}, num bytes: ${airbyteFileUtils.byteCountToDisplaySize(
                    queue.getCurrentMemoryUsage(),
                )}, allocated bytes: ${airbyteFileUtils.byteCountToDisplaySize(queue.getMaxMemoryUsage())}",
            )
        }

        messages.add(globalAsyncStateManager.getMemoryUsageMessage())

        queueInfo.append(join(" | ", messages))

        logger.info { queueInfo.toString() }
    }
}
