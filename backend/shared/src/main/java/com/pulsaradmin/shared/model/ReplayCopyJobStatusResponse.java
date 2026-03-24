package com.pulsaradmin.shared.model;

import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import java.time.Instant;
import java.util.List;

public record ReplayCopyJobStatusResponse(
    String jobId,
    JobType jobType,
    String environmentId,
    JobStatus status,
    String topicName,
    String subscriptionName,
    String destinationTopicName,
    int messageLimit,
    int messagesPerSecond,
    String messageKey,
    java.util.Map<String, String> propertyFilters,
    String filterText,
    String matchField,
    boolean autoReplicateSchema,
    int scannedMessages,
    int matchedMessages,
    int nonMatchedMessages,
    int ackedMessages,
    int nackedMessages,
    int movedMessages,
    int failedMessages,
    int publishedMessages,
    int searchMatchedMessages,
    String searchExportId,
    boolean searchExportReady,
    String searchExportFileName,
    int progressPercent,
    double messagesPerSecondActual,
    long estimatedRemainingSeconds,
    Instant startedAt,
    Instant completedAt,
    String lastMessageId,
    String lastError,
    String statusMessage,
    List<String> warnings,
    Instant createdAt,
    Instant updatedAt) {
}
