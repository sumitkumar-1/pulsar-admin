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
    int matchedMessages,
    int publishedMessages,
    String statusMessage,
    List<String> warnings,
    Instant createdAt,
    Instant updatedAt) {
}
