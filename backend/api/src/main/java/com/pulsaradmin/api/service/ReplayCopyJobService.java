package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class ReplayCopyJobService {
  private final EnvironmentRepository environmentRepository;
  private final EnvironmentSnapshotRepository snapshotRepository;
  private final JobRepository jobRepository;
  private final JobEventRepository jobEventRepository;
  private final PulsarAdminGateway pulsarAdminGateway;
  private final GatewayModeResolver gatewayModeResolver;
  private final MockEnvironmentStore mockEnvironmentStore;
  private final ConcurrentMap<String, ReplayCopyJobStatusResponse> mockJobs = new ConcurrentHashMap<>();

  public ReplayCopyJobService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      JobRepository jobRepository,
      JobEventRepository jobEventRepository,
      PulsarAdminGateway pulsarAdminGateway,
      GatewayModeResolver gatewayModeResolver,
      MockEnvironmentStore mockEnvironmentStore) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.jobRepository = jobRepository;
    this.jobEventRepository = jobEventRepository;
    this.pulsarAdminGateway = pulsarAdminGateway;
    this.gatewayModeResolver = gatewayModeResolver;
    this.mockEnvironmentStore = mockEnvironmentStore;
  }

  public ReplayCopyJobStatusResponse createJob(String environmentId, ReplayCopyJobRequest request) {
    validateRequest(request);
    EnvironmentRecord environment = requireEnvironment(environmentId);
    TopicDetails sourceTopic = requireTopic(environmentId, request.topicName());
    requireSubscription(sourceTopic, request.subscriptionName());

    JobType jobType = normalizeJobType(request.operation());
    Instant now = Instant.now();
    String jobId = "job-" + UUID.randomUUID().toString().substring(0, 8);

    if (isMockMode()) {
      ReplayCopyJobStatusResponse mockResponse = new ReplayCopyJobStatusResponse(
          jobId,
          jobType,
          environment.id(),
          JobStatus.COMPLETED,
          request.topicName(),
          request.subscriptionName(),
          request.destinationTopicName(),
          request.messageLimit(),
          request.messagesPerSecond(),
          blankToNull(request.filterText()),
          Math.min(request.messageLimit(), 24),
          Math.min(request.messageLimit(), 24),
          "Mock replay/copy job completed immediately for demo mode.",
          now,
          now);
      mockJobs.put(jobId, mockResponse);
      return mockResponse;
    }

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
    jobEventRepository.insert(
        queued.id(),
        "QUEUED",
        Map.of(
            "jobType", jobType.name(),
            "topicName", request.topicName(),
            "destinationTopicName", request.destinationTopicName(),
            "messageLimit", request.messageLimit()));

    return toResponse(queued);
  }

  public ReplayCopyJobStatusResponse getJob(String environmentId, String jobId) {
    requireEnvironment(environmentId);

    if (isMockMode()) {
      ReplayCopyJobStatusResponse job = mockJobs.get(jobId);
      if (job == null || !job.environmentId().equals(environmentId)) {
        throw new NotFoundException("Unknown job: " + jobId);
      }
      return job;
    }

    JobRecord job = jobRepository.findById(jobId)
        .orElseThrow(() -> new NotFoundException("Unknown job: " + jobId));

    if (!job.environmentId().equals(environmentId)) {
      throw new NotFoundException("Unknown job: " + jobId);
    }

    return toResponse(job);
  }

  private TopicDetails requireTopic(String environmentId, String topicName) {
    EnvironmentSnapshotRecord snapshot = isMockMode()
        ? mockEnvironmentStore.storeSnapshot(
            environmentId,
            pulsarAdminGateway.syncMetadata(requireEnvironment(environmentId).toDetails()))
        : snapshotRepository.findByEnvironmentId(environmentId)
            .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    return snapshot.topics()
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

  private EnvironmentRecord requireEnvironment(String environmentId) {
    if (isMockMode()) {
      return mockEnvironmentStore.findActiveById(environmentId);
    }

    return environmentRepository.findActiveById(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));
  }

  private boolean isMockMode() {
    return "mock".equals(gatewayModeResolver.resolveMode());
  }
}
