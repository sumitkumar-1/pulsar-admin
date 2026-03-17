package com.pulsaradmin.shared.model;

import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import java.time.Instant;

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
    String filterText,
    int matchedMessages,
    int publishedMessages,
    String statusMessage,
    Instant createdAt,
    Instant updatedAt) {
}
