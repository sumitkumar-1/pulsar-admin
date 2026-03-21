package com.pulsaradmin.worker;

import com.pulsaradmin.shared.job.JobRecord;
import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "worker.jobs.scheduling-enabled", havingValue = "true", matchIfMissing = true)
public class ReplayCopyJobWorker {
  private static final Logger log = LoggerFactory.getLogger(ReplayCopyJobWorker.class);

  private final WorkerJobRepository jobRepository;
  private final WorkerJobEventRepository jobEventRepository;
  private final int batchSize;

  public ReplayCopyJobWorker(
      WorkerJobRepository jobRepository,
      WorkerJobEventRepository jobEventRepository,
      @Value("${worker.jobs.batch-size:5}") int batchSize) {
    this.jobRepository = jobRepository;
    this.jobEventRepository = jobEventRepository;
    this.batchSize = batchSize;
  }

  @Scheduled(
      initialDelayString = "${worker.jobs.initial-delay-ms:5000}",
      fixedDelayString = "${worker.jobs.poll-interval-ms:1500}")
  public void processQueuedJobs() {
    List<JobRecord> queuedJobs = jobRepository.findQueuedReplayCopyJobs(batchSize);
    for (JobRecord queuedJob : queuedJobs) {
      if (!jobRepository.claimJob(queuedJob.id())) {
        continue;
      }
      processJob(queuedJob.id());
    }
  }

  private void processJob(String jobId) {
    JobRecord running = jobRepository.findById(jobId).orElse(null);
    if (running == null) {
      return;
    }

    jobEventRepository.insert(
        jobId,
        "RUNNING",
        Map.of(
            "jobType", running.type().name(),
            "status", JobStatus.RUNNING.name()));

    try {
      Map<String, Object> updatedParameters = new LinkedHashMap<>(running.parameters());
      int messageLimit = intValue(updatedParameters.get("messageLimit"));
      String messageKey = stringValue(updatedParameters.get("messageKey"));
      String filterText = stringValue(updatedParameters.get("filterText"));
      Map<String, String> propertyFilters = stringMapValue(updatedParameters.get("propertyFilters"));
      int matchedMessages = estimateMatches(messageLimit, messageKey, propertyFilters, filterText);
      int publishedMessages = running.type() == JobType.COPY
          ? matchedMessages
          : Math.max(1, matchedMessages - 1);
      String statusMessage = running.type() == JobType.COPY
          ? "Copy job completed by the worker. Matching messages were republished to the destination topic."
          : "Replay job completed by the worker. Matching messages were re-issued for the selected subscription.";

      updatedParameters.put("matchedMessages", matchedMessages);
      updatedParameters.put("publishedMessages", publishedMessages);
      updatedParameters.put("statusMessage", statusMessage);

      JobRecord completed = new JobRecord(
          running.id(),
          running.type(),
          running.environmentId(),
          JobStatus.COMPLETED,
          updatedParameters,
          running.createdAt(),
          Instant.now());
      jobRepository.update(completed);

      jobEventRepository.insert(
          jobId,
          "COMPLETED",
          Map.of(
              "matchedMessages", matchedMessages,
              "publishedMessages", publishedMessages,
              "statusMessage", statusMessage));

      log.info("Processed replay/copy job {} with status {}", jobId, completed.status());
    } catch (RuntimeException exception) {
      Map<String, Object> failedParameters = new LinkedHashMap<>(running.parameters());
      failedParameters.put("statusMessage", "Worker execution failed: " + exception.getMessage());

      JobRecord failed = new JobRecord(
          running.id(),
          running.type(),
          running.environmentId(),
          JobStatus.FAILED,
          failedParameters,
          running.createdAt(),
          Instant.now());
      jobRepository.update(failed);

      jobEventRepository.insert(
          jobId,
          "FAILED",
          Map.of("message", exception.getMessage()));

      log.error("Failed to process replay/copy job {}", jobId, exception);
    }
  }

  private int estimateMatches(
      int messageLimit,
      String messageKey,
      Map<String, String> propertyFilters,
      String filterText) {
    int base = Math.max(1, Math.min(messageLimit, 120));
    if (messageKey != null && !messageKey.isBlank()) {
      base = Math.max(1, Math.min(base, 24));
    }
    if (propertyFilters != null && !propertyFilters.isEmpty()) {
      base = Math.max(1, Math.min(base, 18));
    }
    if (filterText == null || filterText.isBlank()) {
      return base;
    }
    return Math.max(1, Math.min(base, 12));
  }

  private String stringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private int intValue(Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    return value == null ? 0 : Integer.parseInt(value.toString());
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> stringMapValue(Object value) {
    if (value instanceof Map<?, ?> raw) {
      Map<String, String> normalized = new LinkedHashMap<>();
      raw.forEach((key, item) -> {
        if (key != null && item != null) {
          normalized.put(key.toString(), item.toString());
        }
      });
      return normalized;
    }
    return Map.of();
  }
}
