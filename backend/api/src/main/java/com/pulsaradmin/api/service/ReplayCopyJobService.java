package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.job.JobRecord;
import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class ReplayCopyJobService {
  private final EnvironmentRepository environmentRepository;
  private final EnvironmentSnapshotRepository snapshotRepository;
  private final JobRepository jobRepository;

  public ReplayCopyJobService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      JobRepository jobRepository) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.jobRepository = jobRepository;
  }

  public ReplayCopyJobStatusResponse createJob(String environmentId, ReplayCopyJobRequest request) {
    EnvironmentRecord environment = environmentRepository.findActiveById(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));

    validateRequest(request);
    TopicDetails sourceTopic = requireTopic(environmentId, request.topicName());
    requireSubscription(sourceTopic, request.subscriptionName());

    JobType jobType = normalizeJobType(request.operation());
    Instant now = Instant.now();
    String jobId = "job-" + UUID.randomUUID().toString().substring(0, 8);

    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("topicName", request.topicName());
    parameters.put("subscriptionName", request.subscriptionName());
    parameters.put("destinationTopicName", request.destinationTopicName());
    parameters.put("messageLimit", request.messageLimit());
    parameters.put("messagesPerSecond", request.messagesPerSecond());
    parameters.put("filterText", blankToNull(request.filterText()));
    parameters.put("reason", request.reason());
    parameters.put("statusMessage", "Queued for worker pickup.");
    parameters.put("matchedMessages", 0);
    parameters.put("publishedMessages", 0);

    JobRecord queued = new JobRecord(
        jobId,
        jobType,
        environment.id(),
        JobStatus.QUEUED,
        parameters,
        now,
        now);
    jobRepository.insert(queued);

    int matchedMessages = estimateMatches(request.messageLimit(), request.filterText());
    int publishedMessages = jobType == JobType.COPY ? matchedMessages : Math.max(1, matchedMessages - 1);
    String statusMessage = jobType == JobType.COPY
        ? "Copy job completed in mock mode. Matching messages were republished to the destination topic."
        : "Replay job completed in mock mode. Matching messages were re-issued for the selected subscription.";

    Map<String, Object> completedParameters = new LinkedHashMap<>(parameters);
    completedParameters.put("statusMessage", statusMessage);
    completedParameters.put("matchedMessages", matchedMessages);
    completedParameters.put("publishedMessages", publishedMessages);

    JobRecord completed = new JobRecord(
        queued.id(),
        queued.type(),
        queued.environmentId(),
        JobStatus.COMPLETED,
        completedParameters,
        queued.createdAt(),
        Instant.now());
    jobRepository.update(completed);

    return toResponse(queued);
  }

  public ReplayCopyJobStatusResponse getJob(String environmentId, String jobId) {
    environmentRepository.findActiveById(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));

    JobRecord job = jobRepository.findById(jobId)
        .orElseThrow(() -> new NotFoundException("Unknown job: " + jobId));

    if (!job.environmentId().equals(environmentId)) {
      throw new NotFoundException("Unknown job: " + jobId);
    }

    return toResponse(job);
  }

  private TopicDetails requireTopic(String environmentId, String topicName) {
    return snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId))
        .topics()
        .stream()
        .filter(topic -> topic.fullName().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + topicName));
  }

  private void requireSubscription(TopicDetails topic, String subscriptionName) {
    boolean exists = topic.subscriptions().stream().anyMatch(subscriptionName::equals);
    if (!exists) {
      throw new NotFoundException("Unknown subscription: " + subscriptionName);
    }
  }

  private void validateRequest(ReplayCopyJobRequest request) {
    if (request.messageLimit() < 1 || request.messageLimit() > 5000) {
      throw new BadRequestException("Message limit must be between 1 and 5000.");
    }

    if (request.messagesPerSecond() < 1 || request.messagesPerSecond() > 1000) {
      throw new BadRequestException("Rate limit must be between 1 and 1000 messages per second.");
    }

    if (request.topicName().equals(request.destinationTopicName())) {
      throw new BadRequestException("Destination topic must be different from the source topic.");
    }
  }

  private JobType normalizeJobType(String operation) {
    String normalized = operation == null ? "" : operation.trim().toUpperCase();
    return switch (normalized) {
      case "COPY" -> JobType.COPY;
      case "REPLAY" -> JobType.REPLAY;
      default -> throw new BadRequestException("Operation must be REPLAY or COPY.");
    };
  }

  private int estimateMatches(int messageLimit, String filterText) {
    if (filterText == null || filterText.isBlank()) {
      return Math.max(1, Math.min(messageLimit, 120));
    }
    return Math.max(1, Math.min(messageLimit, 36));
  }

  private ReplayCopyJobStatusResponse toResponse(JobRecord job) {
    Map<String, Object> parameters = job.parameters();
    return new ReplayCopyJobStatusResponse(
        job.id(),
        job.type(),
        job.environmentId(),
        job.status(),
        stringValue(parameters.get("topicName")),
        stringValue(parameters.get("subscriptionName")),
        stringValue(parameters.get("destinationTopicName")),
        intValue(parameters.get("messageLimit")),
        intValue(parameters.get("messagesPerSecond")),
        stringValue(parameters.get("filterText")),
        intValue(parameters.get("matchedMessages")),
        intValue(parameters.get("publishedMessages")),
        stringValue(parameters.get("statusMessage")),
        job.createdAt(),
        job.updatedAt());
  }

  private String stringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private int intValue(Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value == null) {
      return 0;
    }
    return Integer.parseInt(value.toString());
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }
}
