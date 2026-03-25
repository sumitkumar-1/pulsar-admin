package com.pulsaradmin.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.shared.job.JobRecord;
import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@ConditionalOnProperty(name = "worker.jobs.scheduling-enabled", havingValue = "true", matchIfMissing = true)
public class ReplayCopyJobWorker {
  private static final Logger log = LoggerFactory.getLogger(ReplayCopyJobWorker.class);

  private final WorkerJobRepository jobRepository;
  private final WorkerJobEventRepository jobEventRepository;
  private final WorkerEnvironmentRepository environmentRepository;
  private final WorkerReplayCopyJobCriteriaRepository criteriaRepository;
  private final WorkerReplayCopyJobSearchResultRepository searchResultRepository;
  private final ObjectMapper objectMapper;
  private final int batchSize;
  private final int receiveTimeoutMs;
  private final int idleTimeoutSeconds;
  private final int heartbeatMessages;
  private final int heartbeatSeconds;
  private final int searchExportMaxResults;

  public ReplayCopyJobWorker(
      WorkerJobRepository jobRepository,
      WorkerJobEventRepository jobEventRepository,
      WorkerEnvironmentRepository environmentRepository,
      WorkerReplayCopyJobCriteriaRepository criteriaRepository,
      WorkerReplayCopyJobSearchResultRepository searchResultRepository,
      ObjectMapper objectMapper,
      @Value("${worker.jobs.batch-size:5}") int batchSize,
      @Value("${worker.jobs.receive-timeout-ms:500}") int receiveTimeoutMs,
      @Value("${worker.jobs.idle-timeout-seconds:15}") int idleTimeoutSeconds,
      @Value("${worker.jobs.heartbeat-messages:100}") int heartbeatMessages,
      @Value("${worker.jobs.heartbeat-seconds:5}") int heartbeatSeconds,
      @Value("${worker.jobs.search-export-max-results:10000}") int searchExportMaxResults) {
    this.jobRepository = jobRepository;
    this.jobEventRepository = jobEventRepository;
    this.environmentRepository = environmentRepository;
    this.criteriaRepository = criteriaRepository;
    this.searchResultRepository = searchResultRepository;
    this.objectMapper = objectMapper;
    this.batchSize = batchSize;
    this.receiveTimeoutMs = receiveTimeoutMs;
    this.idleTimeoutSeconds = idleTimeoutSeconds;
    this.heartbeatMessages = heartbeatMessages;
    this.heartbeatSeconds = heartbeatSeconds;
    this.searchExportMaxResults = searchExportMaxResults;
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
      WorkerEnvironmentRecord environment = environmentRepository.findActiveById(running.environmentId())
          .orElseThrow(() -> new IllegalStateException("Unknown environment for job: " + running.environmentId()));
      List<Map<String, String>> criteriaRows = criteriaRepository.findRowsByJobId(jobId);
      processReplayCopyJob(running, updatedParameters, environment, criteriaRows);
    } catch (RuntimeException exception) {
      Map<String, Object> failedParameters = new LinkedHashMap<>(running.parameters());
      failedParameters.put("statusMessage", "Worker execution failed: " + exception.getMessage());
      failedParameters.put("lastError", exception.getMessage());
      failedParameters.put("completedAt", Instant.now().toString());

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

  private void processReplayCopyJob(
      JobRecord running,
      Map<String, Object> parameters,
      WorkerEnvironmentRecord environment,
      List<Map<String, String>> criteriaRows) {
    if (environment.brokerUrl() != null && environment.brokerUrl().startsWith("mock://")) {
      processMockReplayCopyJob(running, parameters, criteriaRows);
      return;
    }

    String topicName = stringValue(parameters.get("topicName"));
    String subscriptionName = stringValue(parameters.get("subscriptionName"));
    String destinationTopicName = stringValue(parameters.get("destinationTopicName"));
    int messageLimit = intValue(parameters.get("messageLimit"));
    int configuredRate = Math.max(1, intValue(parameters.get("messagesPerSecond")));
    boolean autoReplicateSchema = booleanValue(parameters.get("autoReplicateSchema"));
    boolean searchOnly = running.type() == JobType.SEARCH;
    String searchExportId = searchOnly ? "search-export-" + running.id() : null;
    String searchExportFileName = searchOnly ? "search-results-" + running.id() + ".json" : null;

    Instant startedAt = Instant.now();
    parameters.put("startedAt", startedAt.toString());
    parameters.put("statusMessage", "Running replay/copy scan.");
    if (searchOnly) {
      parameters.put("searchExportId", searchExportId);
      parameters.put("searchExportFileName", searchExportFileName);
      parameters.put("searchExportReady", false);
    }
    persistRunningState(running, parameters);

    long scanned = 0;
    long matched = 0;
    long nonMatched = 0;
    long acked = 0;
    long nacked = 0;
    long moved = 0;
    long failed = 0;
    long searchMatched = 0;
    long searchStored = 0;
    String lastMessageId = null;
    String lastError = null;
    long startedAtMs = System.currentTimeMillis();
    long lastProgressAtMs = startedAtMs;
    long lastMessageAtMs = startedAtMs;
    long nextAllowedAtNs = System.nanoTime();
    long minGapNs = Math.max(0L, 1_000_000_000L / configuredRate);
    boolean idleTimeoutReached = false;

    try (PulsarClient client = buildClient(environment);
         Consumer<byte[]> consumer = client.newConsumer()
             .topic(topicName)
             .subscriptionName(subscriptionName)
             .subscriptionType(SubscriptionType.Exclusive)
             .subscribe()) {
      Producer<byte[]> producer = null;
      try {
        if (running.type() == JobType.COPY) {
          if (autoReplicateSchema) {
            jobEventRepository.insert(running.id(), "SCHEMA_REPLICATION_STARTED", Map.of(
                "topicName", topicName,
                "destinationTopicName", destinationTopicName));
            try {
              replicateSchema(environment, topicName, destinationTopicName);
              jobEventRepository.insert(running.id(), "SCHEMA_REPLICATION_SUCCEEDED", Map.of(
                  "topicName", topicName,
                  "destinationTopicName", destinationTopicName));
            } catch (RuntimeException schemaException) {
              jobEventRepository.insert(running.id(), "SCHEMA_REPLICATION_FAILED", Map.of(
                  "topicName", topicName,
                  "destinationTopicName", destinationTopicName,
                  "message", schemaException.getMessage()));
              throw schemaException;
            }
          }
          producer = client.newProducer()
              .topic(destinationTopicName)
              .create();
        }

        while (scanned < messageLimit) {
          Message<byte[]> message = consumer.receive(receiveTimeoutMs, TimeUnit.MILLISECONDS);
          long nowMs = System.currentTimeMillis();
          if (message == null) {
            if ((nowMs - lastMessageAtMs) >= idleTimeoutSeconds * 1000L) {
              idleTimeoutReached = true;
              break;
            }
            maybeEmitProgress(running, parameters, startedAtMs, nowMs, messageLimit, scanned, matched, nonMatched, acked, nacked, moved, failed, searchMatched, configuredRate, lastMessageId, lastError);
            continue;
          }

          lastMessageAtMs = nowMs;
          scanned++;
          lastMessageId = String.valueOf(message.getMessageId());
          boolean matches = matchesMessage(message, criteriaRows);

          if (matches) {
            matched++;
            if (searchOnly) {
              searchMatched++;
              if (searchStored < searchExportMaxResults) {
                String payload = message.getData() == null ? "" : new String(message.getData(), StandardCharsets.UTF_8);
                String matchedFieldValue = extractMatchingCriteriaSummary(payload, criteriaRows);
                searchResultRepository.insert(
                    running.id(),
                    String.valueOf(message.getMessageId()),
                    message.hasKey() ? message.getKey() : null,
                    message.getProperties() == null ? Map.of() : message.getProperties(),
                    payload,
                    matchedFieldValue);
                searchStored++;
                if (searchStored == 1) {
                  jobEventRepository.insert(running.id(), "SEARCH_SAMPLE_READY", Map.of(
                      "searchExportId", searchExportId,
                      "searchExportFileName", searchExportFileName));
                }
              }
            } else if (running.type() == JobType.COPY) {
              try {
                var builder = producer.newMessage().value(message.getData());
                if (message.hasKey()) {
                  builder.key(message.getKey());
                }
                if (message.getProperties() != null && !message.getProperties().isEmpty()) {
                  builder.properties(message.getProperties());
                }
                builder.send();
                moved++;
                consumer.acknowledge(message);
                acked++;
              } catch (Exception copyError) {
                failed++;
                nacked++;
                lastError = copyError.getMessage();
                consumer.negativeAcknowledge(message);
              }
            } else {
              try {
                consumer.acknowledge(message);
                acked++;
              } catch (Exception acknowledgeError) {
                failed++;
                nacked++;
                lastError = acknowledgeError.getMessage();
                consumer.negativeAcknowledge(message);
              }
            }
          } else {
            nonMatched++;
            if (!searchOnly) {
              nacked++;
              consumer.negativeAcknowledge(message);
            }
          }

          if (minGapNs > 0) {
            nextAllowedAtNs += minGapNs;
            long remainingNs = nextAllowedAtNs - System.nanoTime();
            if (remainingNs > 0) {
              try {
                TimeUnit.NANOSECONDS.sleep(remainingNs);
              } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while rate limiting replay/copy processing.", interruptedException);
              }
            } else {
              nextAllowedAtNs = System.nanoTime();
            }
          }

          if ((scanned % Math.max(1, heartbeatMessages) == 0)
              || (nowMs - lastProgressAtMs) >= Math.max(1, heartbeatSeconds) * 1000L) {
            maybeEmitProgress(running, parameters, startedAtMs, nowMs, messageLimit, scanned, matched, nonMatched, acked, nacked, moved, failed, searchMatched, configuredRate, lastMessageId, lastError);
            lastProgressAtMs = nowMs;
          }
        }
      } finally {
        if (producer != null) {
          try {
            producer.close();
          } catch (Exception closeException) {
            log.warn("Unable to close replay/copy producer cleanly for job {}", running.id(), closeException);
          }
        }
      }
    } catch (Exception exception) {
      throw new RuntimeException("Replay/copy processing failed: " + exception.getMessage(), exception);
    }

    long completedAtMs = System.currentTimeMillis();
    Instant completedAt = Instant.ofEpochMilli(completedAtMs);
    double actualRate = elapsedRate(scanned, startedAtMs, completedAtMs);
    int progressPercent = computeProgressPercent(scanned, messageLimit);
    long estimatedRemainingSeconds = estimateRemainingSeconds(messageLimit, scanned, actualRate);

    parameters.put("scannedMessages", scanned);
    parameters.put("matchedMessages", matched);
    parameters.put("nonMatchedMessages", nonMatched);
    parameters.put("ackedMessages", acked);
    parameters.put("nackedMessages", nacked);
    parameters.put("movedMessages", moved);
    parameters.put("failedMessages", failed);
    parameters.put("publishedMessages", moved);
    parameters.put("searchMatchedMessages", searchMatched);
    parameters.put("searchExportId", searchExportId);
    parameters.put("searchExportReady", searchOnly);
    parameters.put("searchExportFileName", searchExportFileName);
    parameters.put("progressPercent", progressPercent);
    parameters.put("messagesPerSecondActual", actualRate);
    parameters.put("estimatedRemainingSeconds", estimatedRemainingSeconds);
    parameters.put("completedAt", completedAt.toString());
    parameters.put("lastMessageId", lastMessageId);
    parameters.put("lastError", lastError);
    parameters.put(
        "statusMessage",
        searchOnly
            ? "Search job completed. Export is available for matched messages."
            : (idleTimeoutReached
                ? "Job completed after idle timeout window with no additional messages."
                : "Job reached the configured message limit and completed."));

    JobRecord completed = new JobRecord(
        running.id(),
        running.type(),
        running.environmentId(),
        JobStatus.COMPLETED,
        parameters,
        running.createdAt(),
        completedAt);
    jobRepository.update(completed);
    jobEventRepository.insert(
        running.id(),
        "COMPLETED",
        Map.of(
            "scannedMessages", scanned,
            "matchedMessages", matched,
            "nonMatchedMessages", nonMatched,
            "ackedMessages", acked,
            "nackedMessages", nacked,
            "movedMessages", moved,
            "failedMessages", failed,
            "searchMatchedMessages", searchMatched,
            "statusMessage", stringValue(parameters.get("statusMessage"))));
    if (searchOnly) {
      jobEventRepository.insert(running.id(), "SEARCH_EXPORT_READY", Map.of(
          "searchExportId", searchExportId,
          "searchExportFileName", searchExportFileName,
          "matchedMessages", searchMatched,
          "storedMessages", searchStored));
    }
    log.info("Processed replay/copy job {} with status {}", running.id(), completed.status());
  }

  private void processMockReplayCopyJob(
      JobRecord running,
      Map<String, Object> parameters,
      List<Map<String, String>> criteriaRows) {
    int messageLimit = intValue(parameters.get("messageLimit"));
    int criteriaRowCount = criteriaRows == null ? 0 : criteriaRows.size();
    int simulatedScanned = Math.max(1, Math.min(messageLimit, criteriaRowCount == 0 ? 12 : criteriaRowCount));
    int simulatedMatched = Math.max(1, Math.min(simulatedScanned, 12));
    int simulatedMoved = running.type() == JobType.COPY ? simulatedMatched : 0;
    int simulatedNonMatched = Math.max(0, simulatedScanned - simulatedMatched);
    int simulatedSearchMatched = running.type() == JobType.SEARCH ? simulatedMatched : 0;
    Instant now = Instant.now();

    parameters.put("startedAt", now.toString());
    parameters.put("completedAt", now.toString());
    parameters.put("scannedMessages", simulatedScanned);
    parameters.put("matchedMessages", simulatedMatched);
    parameters.put("nonMatchedMessages", simulatedNonMatched);
    parameters.put("ackedMessages", running.type() == JobType.SEARCH ? 0 : simulatedMatched);
    parameters.put("nackedMessages", running.type() == JobType.SEARCH ? 0 : simulatedNonMatched);
    parameters.put("movedMessages", simulatedMoved);
    parameters.put("failedMessages", 0);
    parameters.put("publishedMessages", simulatedMoved);
    parameters.put("searchMatchedMessages", simulatedSearchMatched);
    parameters.put("searchExportId", running.type() == JobType.SEARCH ? "search-export-" + running.id() : null);
    parameters.put("searchExportReady", running.type() == JobType.SEARCH);
    parameters.put("searchExportFileName", running.type() == JobType.SEARCH ? "search-results-" + running.id() + ".json" : null);
    parameters.put("progressPercent", computeProgressPercent(simulatedScanned, Math.max(1, messageLimit)));
    parameters.put("messagesPerSecondActual", 50d);
    parameters.put("estimatedRemainingSeconds", 0L);
    parameters.put("lastMessageId", "mock-message-" + simulatedScanned);
    parameters.put("lastError", null);
    parameters.put("statusMessage", "Mock worker execution completed.");

    JobRecord completed = new JobRecord(
        running.id(),
        running.type(),
        running.environmentId(),
        JobStatus.COMPLETED,
        parameters,
        running.createdAt(),
        now);
    jobRepository.update(completed);
    jobEventRepository.insert(
        running.id(),
        "COMPLETED",
        Map.of(
            "scannedMessages", simulatedScanned,
            "matchedMessages", simulatedMatched,
            "publishedMessages", simulatedMoved,
            "statusMessage", "Mock worker execution completed."));
  }

  private void maybeEmitProgress(
      JobRecord running,
      Map<String, Object> parameters,
      long startedAtMs,
      long nowMs,
      int messageLimit,
      long scanned,
      long matched,
      long nonMatched,
      long acked,
      long nacked,
      long moved,
      long failed,
      long searchMatched,
      int configuredRate,
      String lastMessageId,
      String lastError) {
    double actualRate = elapsedRate(scanned, startedAtMs, nowMs);
    int progressPercent = computeProgressPercent(scanned, messageLimit);
    long estimatedRemainingSeconds = estimateRemainingSeconds(messageLimit, scanned, actualRate);
    parameters.put("scannedMessages", scanned);
    parameters.put("matchedMessages", matched);
    parameters.put("nonMatchedMessages", nonMatched);
    parameters.put("ackedMessages", acked);
    parameters.put("nackedMessages", nacked);
    parameters.put("movedMessages", moved);
    parameters.put("failedMessages", failed);
    parameters.put("publishedMessages", moved);
    parameters.put("searchMatchedMessages", searchMatched);
    parameters.put("progressPercent", progressPercent);
    parameters.put("messagesPerSecondActual", actualRate);
    parameters.put("estimatedRemainingSeconds", estimatedRemainingSeconds);
    parameters.put("lastMessageId", lastMessageId);
    parameters.put("lastError", lastError);
    parameters.put(
        "statusMessage",
        "Running replay/copy scan at approximately " + String.format("%.2f", actualRate) + " msg/s.");
    persistRunningState(running, parameters);
    Map<String, Object> progressDetails = new LinkedHashMap<>();
    progressDetails.put("messageLimit", messageLimit);
    progressDetails.put("configuredMessagesPerSecond", configuredRate);
    progressDetails.put("scannedMessages", scanned);
    progressDetails.put("matchedMessages", matched);
    progressDetails.put("nonMatchedMessages", nonMatched);
    progressDetails.put("ackedMessages", acked);
    progressDetails.put("nackedMessages", nacked);
    progressDetails.put("movedMessages", moved);
    progressDetails.put("failedMessages", failed);
    progressDetails.put("searchMatchedMessages", searchMatched);
    progressDetails.put("progressPercent", progressPercent);
    progressDetails.put("messagesPerSecondActual", actualRate);
    progressDetails.put("estimatedRemainingSeconds", estimatedRemainingSeconds);
    jobEventRepository.insert(
        running.id(),
        "PROGRESS",
        progressDetails);
  }

  private void persistRunningState(JobRecord running, Map<String, Object> parameters) {
    jobRepository.update(
        new JobRecord(
            running.id(),
            running.type(),
            running.environmentId(),
            JobStatus.RUNNING,
            parameters,
            running.createdAt(),
            Instant.now()));
  }

  private boolean matchesMessage(
      Message<byte[]> message,
      List<Map<String, String>> criteriaRows) {
    if (criteriaRows == null || criteriaRows.isEmpty()) {
      return true;
    }
    String payload = message.getData() == null ? "" : new String(message.getData(), StandardCharsets.UTF_8);
    JsonNode root = parsePayload(payload);
    if (root == null || !root.isObject()) {
      return false;
    }

    for (Map<String, String> row : criteriaRows) {
      if (row == null || row.isEmpty()) {
        continue;
      }
      boolean rowMatches = true;
      for (Map.Entry<String, String> entry : row.entrySet()) {
        JsonNode fieldNode = root.path(entry.getKey());
        if (fieldNode.isMissingNode() || fieldNode.isNull()) {
          rowMatches = false;
          break;
        }
        String fieldValue = fieldNode.isValueNode() ? fieldNode.asText() : fieldNode.toString();
        if (!entry.getValue().equals(fieldValue)) {
          rowMatches = false;
          break;
        }
      }
      if (rowMatches) {
        return true;
      }
    }
    return false;
  }

  private JsonNode parsePayload(String payload) {
    if (payload == null || payload.isBlank()) {
      return null;
    }
    try {
      return objectMapper.readTree(payload);
    } catch (Exception exception) {
      return null;
    }
  }

  private String extractMatchingCriteriaSummary(String payload, List<Map<String, String>> criteriaRows) {
    if (criteriaRows == null || criteriaRows.isEmpty()) {
      return null;
    }
    JsonNode root = parsePayload(payload);
    if (root == null || !root.isObject()) {
      return null;
    }
    for (Map<String, String> row : criteriaRows) {
      if (row == null || row.isEmpty()) {
        continue;
      }
      boolean rowMatches = true;
      for (Map.Entry<String, String> entry : row.entrySet()) {
        JsonNode fieldNode = root.path(entry.getKey());
        if (fieldNode.isMissingNode() || fieldNode.isNull()) {
          rowMatches = false;
          break;
        }
        String fieldValue = fieldNode.isValueNode() ? fieldNode.asText() : fieldNode.toString();
        if (!entry.getValue().equals(fieldValue)) {
          rowMatches = false;
          break;
        }
      }
      if (rowMatches) {
        return row.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .reduce((left, right) -> left + "," + right)
            .orElse(null);
      }
    }
    return null;
  }

  private void replicateSchema(WorkerEnvironmentRecord environment, String sourceTopicName, String destinationTopicName) {
    if (destinationTopicName == null || destinationTopicName.isBlank()) {
      throw new RuntimeException("Destination topic is required for schema replication.");
    }
    TopicPath sourcePath = parseTopicPath(sourceTopicName);
    TopicPath destinationPath = parseTopicPath(destinationTopicName);
    RestClient restClient = buildRestClient(environment);

    try {
      JsonNode sourceSchema = restClient.get()
          .uri(environment.adminUrl() + schemaPath(sourcePath))
          .retrieve()
          .body(JsonNode.class);
      if (sourceSchema == null || sourceSchema.path("data").asText("").isBlank()) {
        return;
      }

      restClient.post()
          .uri(environment.adminUrl() + schemaPath(destinationPath))
          .body(Map.of(
              "type", sourceSchema.path("type").asText("JSON"),
              "schema", sourceSchema.path("data").asText("")))
          .retrieve()
          .toBodilessEntity();

      JsonNode compatibility = restClient.get()
          .uri(environment.adminUrl() + schemaCompatibilityPath(sourcePath.tenant(), sourcePath.namespace()))
          .retrieve()
          .body(JsonNode.class);
      String compatibilityStrategy = compatibility == null ? null : compatibility.asText("");
      if (compatibilityStrategy != null && !compatibilityStrategy.isBlank()) {
        restClient.put()
            .uri(environment.adminUrl() + schemaCompatibilityPath(destinationPath.tenant(), destinationPath.namespace()))
            .body(compatibilityStrategy)
            .retrieve()
            .toBodilessEntity();
      }
    } catch (Exception exception) {
      throw new RuntimeException("Unable to replicate schema: " + exception.getMessage(), exception);
    }
  }

  private RestClient buildRestClient(WorkerEnvironmentRecord environment) {
    RestClient.Builder builder = RestClient.builder();
    String authHeader = resolveAuthorizationHeader(environment);
    if (authHeader != null && !authHeader.isBlank()) {
      builder.defaultHeader("Authorization", authHeader);
    }
    return builder.build();
  }

  private PulsarClient buildClient(WorkerEnvironmentRecord environment) throws Exception {
    var builder = PulsarClient.builder()
        .serviceUrl(environment.brokerUrl())
        .enableTls(environment.tlsEnabled())
        .operationTimeout(20, TimeUnit.SECONDS);
    String authMode = environment.authMode() == null ? "" : environment.authMode().trim().toLowerCase();
    if ("token".equals(authMode)) {
      String token = resolveToken(environment);
      builder.authentication(AuthenticationFactory.token(token));
    } else if ("basic".equals(authMode)) {
      String[] credentials = resolveBasicCredentials(environment);
      builder.authentication(AuthenticationFactory.create(
          "org.apache.pulsar.client.impl.auth.AuthenticationBasic",
          Map.of("userId", credentials[0], "password", credentials[1])));
    }
    return builder.build();
  }

  private String resolveAuthorizationHeader(WorkerEnvironmentRecord environment) {
    String authMode = environment.authMode() == null ? "" : environment.authMode().trim().toLowerCase();
    if ("none".equals(authMode) || authMode.isBlank()) {
      return null;
    }
    String credential = resolveCredentialValue(environment);
    if ("token".equals(authMode)) {
      if (credential.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
        return credential;
      }
      if (credential.regionMatches(true, 0, "token:", 0, "token:".length())) {
        return "Bearer " + credential.substring("token:".length()).trim();
      }
      return "Bearer " + credential;
    }
    if ("basic".equals(authMode)) {
      if (credential.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
        return credential;
      }
      String encoded = Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
      return "Basic " + encoded;
    }
    return null;
  }

  private String resolveToken(WorkerEnvironmentRecord environment) {
    String credential = resolveCredentialValue(environment);
    if (credential.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
      return credential.substring("Bearer ".length()).trim();
    }
    if (credential.regionMatches(true, 0, "token:", 0, "token:".length())) {
      return credential.substring("token:".length()).trim();
    }
    return credential;
  }

  private String[] resolveBasicCredentials(WorkerEnvironmentRecord environment) {
    String credential = resolveCredentialValue(environment);
    String decoded = credential;
    if (credential.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
      decoded = new String(Base64.getDecoder().decode(credential.substring("Basic ".length()).trim()), StandardCharsets.UTF_8);
    }
    int separator = decoded.indexOf(':');
    if (separator <= 0 || separator >= decoded.length() - 1) {
      throw new RuntimeException("Basic credentials must be provided as username:password.");
    }
    return new String[] {decoded.substring(0, separator), decoded.substring(separator + 1)};
  }

  private String resolveCredentialValue(WorkerEnvironmentRecord environment) {
    String reference = environment.credentialReference();
    if (reference == null || reference.isBlank()) {
      throw new RuntimeException("Credential reference is required for auth mode " + environment.authMode() + ".");
    }
    String trimmed = reference.trim();
    if (trimmed.startsWith("env://")) {
      String variableName = trimmed.substring("env://".length()).trim();
      String value = System.getenv(variableName);
      if (value == null || value.isBlank()) {
        throw new RuntimeException("Environment variable " + variableName + " is not set.");
      }
      return value.trim();
    }
    if (trimmed.startsWith("inline://")) {
      return trimmed.substring("inline://".length()).trim();
    }
    return trimmed;
  }

  private TopicPath parseTopicPath(String topicName) {
    if (topicName == null || topicName.isBlank()) {
      throw new RuntimeException("Topic name is required.");
    }
    String normalized = topicName;
    if (normalized.startsWith("persistent://")) {
      normalized = normalized.substring("persistent://".length());
    } else if (normalized.startsWith("non-persistent://")) {
      normalized = normalized.substring("non-persistent://".length());
    }
    String[] parts = normalized.split("/", 3);
    if (parts.length != 3) {
      throw new RuntimeException("Topic name must be in <domain>://tenant/namespace/topic format.");
    }
    String domain = topicName.startsWith("non-persistent://") ? "non-persistent" : "persistent";
    return new TopicPath(domain, parts[0], parts[1], parts[2]);
  }

  private String schemaPath(TopicPath topicPath) {
    return "/admin/v2/schemas/" + topicPath.tenant() + "/" + topicPath.namespace() + "/" + topicPath.topic() + "/schema";
  }

  private String schemaCompatibilityPath(String tenant, String namespace) {
    return "/admin/v2/namespaces/" + tenant + "/" + namespace + "/schemaCompatibilityStrategy";
  }

  private double elapsedRate(long scanned, long startedAtMs, long nowMs) {
    long elapsedMs = Math.max(1L, nowMs - startedAtMs);
    return scanned * 1000d / elapsedMs;
  }

  private int computeProgressPercent(long scanned, int messageLimit) {
    if (messageLimit <= 0) {
      return 0;
    }
    return (int) Math.max(0, Math.min(100, Math.round((scanned * 100d) / messageLimit)));
  }

  private long estimateRemainingSeconds(int messageLimit, long scanned, double actualRate) {
    if (actualRate <= 0d) {
      return 0L;
    }
    long remaining = Math.max(0, messageLimit - scanned);
    return (long) Math.ceil(remaining / actualRate);
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

  private boolean booleanValue(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value == null) {
      return false;
    }
    return Boolean.parseBoolean(value.toString());
  }

  private record TopicPath(String domain, String tenant, String namespace, String topic) {
  }
}
