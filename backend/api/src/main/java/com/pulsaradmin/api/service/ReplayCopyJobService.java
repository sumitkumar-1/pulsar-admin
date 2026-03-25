package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.job.JobRecord;
import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import com.pulsaradmin.shared.model.ReplayCopyJobEventResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopySearchExportResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.TopicDetails;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final ReplayCopyJobCriteriaRepository replayCopyJobCriteriaRepository;
  private final ReplayCopyJobSearchResultRepository replayCopyJobSearchResultRepository;
  private final PulsarAdminGateway pulsarAdminGateway;
  private final GatewayModeResolver gatewayModeResolver;
  private final MockEnvironmentStore mockEnvironmentStore;
  private final ConcurrentMap<String, ReplayCopyJobStatusResponse> mockJobs = new ConcurrentHashMap<>();

  public ReplayCopyJobService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      JobRepository jobRepository,
      JobEventRepository jobEventRepository,
      ReplayCopyJobCriteriaRepository replayCopyJobCriteriaRepository,
      ReplayCopyJobSearchResultRepository replayCopyJobSearchResultRepository,
      PulsarAdminGateway pulsarAdminGateway,
      GatewayModeResolver gatewayModeResolver,
      MockEnvironmentStore mockEnvironmentStore) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.jobRepository = jobRepository;
    this.jobEventRepository = jobEventRepository;
    this.replayCopyJobCriteriaRepository = replayCopyJobCriteriaRepository;
    this.replayCopyJobSearchResultRepository = replayCopyJobSearchResultRepository;
    this.pulsarAdminGateway = pulsarAdminGateway;
    this.gatewayModeResolver = gatewayModeResolver;
    this.mockEnvironmentStore = mockEnvironmentStore;
  }

  public ReplayCopyJobStatusResponse createJob(String environmentId, ReplayCopyJobRequest request) {
    return createJob(environmentId, request, ReplayCopyCriteriaInput.empty());
  }

  public ReplayCopyJobStatusResponse createJob(
      String environmentId,
      ReplayCopyJobRequest request,
      ReplayCopyCriteriaInput criteriaInput) {
    validateRequest(request);
    ReplayCopyCriteriaInput normalizedCriteriaInput =
        criteriaInput == null ? ReplayCopyCriteriaInput.empty() : criteriaInput;
    if (!normalizedCriteriaInput.errors().isEmpty()) {
      throw new BadRequestException("Invalid IDs CSV file: " + String.join(" ", normalizedCriteriaInput.errors()));
    }
    EnvironmentRecord environment = requireEnvironment(environmentId);
    TopicDetails sourceTopic = requireTopic(environmentId, request.topicName());
    requireSubscription(sourceTopic, request.subscriptionName());
    String operationMode = normalizeOperationMode(request);
    JobType jobType = toJobType(operationMode);
    String normalizedDestinationTopic = normalizeDestinationTopic(jobType, request.destinationTopicName());
    List<String> warnings =
        validateSchemaCompatibility(environmentId, sourceTopic, normalizedDestinationTopic, jobType, operationMode);
    List<Map<String, String>> normalizedCriteriaRows = normalizeCriteriaRows(normalizedCriteriaInput.rows());
    int criteriaRowCount = normalizedCriteriaRows.size();
    boolean autoReplicateSchema = request.autoReplicateSchema() == null || request.autoReplicateSchema();
    Instant now = Instant.now();
    String jobId = "job-" + UUID.randomUUID().toString().substring(0, 8);

    if (isMockMode()) {
      int scanned = Math.min(request.messageLimit(), Math.max(1, criteriaRowCount == 0 ? 48 : criteriaRowCount));
      int matched = criteriaRowCount == 0
          ? Math.min(scanned, 24)
          : Math.min(scanned, criteriaRowCount);
      int nacked = Math.max(0, scanned - matched);
      int moved = jobType == JobType.COPY ? matched : 0;
      int searchMatched = jobType == JobType.SEARCH ? matched : 0;
      ReplayCopyJobStatusResponse mockResponse = new ReplayCopyJobStatusResponse(
          jobId,
          jobType,
          environment.id(),
          JobStatus.COMPLETED,
          request.topicName(),
          request.subscriptionName(),
          normalizedDestinationTopic,
          request.messageLimit(),
          request.messagesPerSecond(),
          autoReplicateSchema,
          scanned,
          matched,
          nacked,
          matched,
          nacked,
          moved,
          0,
          moved,
          searchMatched,
          jobType == JobType.SEARCH ? "search-export-" + jobId : null,
          jobType == JobType.SEARCH,
          jobType == JobType.SEARCH ? "search-results-" + jobId + ".json" : null,
          Math.min(100, (int) Math.round((scanned * 100.0) / Math.max(1, request.messageLimit()))),
          request.messagesPerSecond(),
          0L,
          now,
          now,
          "mock-message-" + Math.max(1, scanned),
          null,
          "Mock " + operationMode.toLowerCase().replace('_', '-') + " job completed immediately for demo mode.",
          warnings,
          now,
          now);
      mockJobs.put(jobId, mockResponse);
      return mockResponse;
    }

    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("topicName", request.topicName());
    parameters.put("subscriptionName", request.subscriptionName());
    parameters.put("destinationTopicName", normalizedDestinationTopic);
    parameters.put("messageLimit", request.messageLimit());
    parameters.put("messagesPerSecond", request.messagesPerSecond());
    parameters.put("autoReplicateSchema", autoReplicateSchema);
    parameters.put("operationMode", operationMode);
    parameters.put("criteriaHeaders", normalizedCriteriaInput.headers());
    parameters.put("criteriaRowCount", criteriaRowCount);
    parameters.put("reason", request.reason());
    parameters.put("statusMessage", "Queued for worker pickup.");
    parameters.put("scannedMessages", 0);
    parameters.put("matchedMessages", 0);
    parameters.put("nonMatchedMessages", 0);
    parameters.put("ackedMessages", 0);
    parameters.put("nackedMessages", 0);
    parameters.put("movedMessages", 0);
    parameters.put("failedMessages", 0);
    parameters.put("publishedMessages", 0);
    parameters.put("searchMatchedMessages", 0);
    parameters.put("searchExportId", null);
    parameters.put("searchExportReady", false);
    parameters.put("searchExportFileName", null);
    parameters.put("progressPercent", 0);
    parameters.put("messagesPerSecondActual", 0d);
    parameters.put("estimatedRemainingSeconds", 0L);
    parameters.put("startedAt", null);
    parameters.put("completedAt", null);
    parameters.put("lastMessageId", null);
    parameters.put("lastError", null);
    parameters.put("warnings", warnings);

    JobRecord queued = new JobRecord(
        jobId,
        jobType,
        environment.id(),
        JobStatus.QUEUED,
        parameters,
        now,
        now);
    jobRepository.insert(queued);
    Map<String, Object> queueDetails = new LinkedHashMap<>();
    queueDetails.put("jobType", jobType.name());
    queueDetails.put("operationMode", operationMode);
    queueDetails.put("topicName", request.topicName());
    queueDetails.put("destinationTopicName", normalizedDestinationTopic);
    queueDetails.put("messageLimit", request.messageLimit());
    queueDetails.put("criteriaRowCount", criteriaRowCount);
    jobEventRepository.insert(queued.id(), "QUEUED", queueDetails);
    replayCopyJobCriteriaRepository.insertRows(queued.id(), normalizedCriteriaRows);
    if (criteriaRowCount > 0) {
      Map<String, Object> criteriaDetails = new LinkedHashMap<>();
      criteriaDetails.put("criteriaHeaders", normalizedCriteriaInput.headers());
      criteriaDetails.put("criteriaRowCount", criteriaRowCount);
      jobEventRepository.insert(queued.id(), "CSV_CRITERIA_PARSED", criteriaDetails);
    }

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

  public List<ReplayCopyJobEventResponse> getJobEvents(String environmentId, String jobId) {
    requireEnvironment(environmentId);
    if (isMockMode()) {
      ReplayCopyJobStatusResponse job = mockJobs.get(jobId);
      if (job == null || !job.environmentId().equals(environmentId)) {
        throw new NotFoundException("Unknown job: " + jobId);
      }
      return List.of(
          new ReplayCopyJobEventResponse(1L, jobId, "QUEUED", Map.of("statusMessage", "Queued in mock mode."), job.createdAt()),
          new ReplayCopyJobEventResponse(2L, jobId, "COMPLETED", Map.of("statusMessage", job.statusMessage()), job.updatedAt()));
    }

    JobRecord job = jobRepository.findById(jobId)
        .orElseThrow(() -> new NotFoundException("Unknown job: " + jobId));
    if (!job.environmentId().equals(environmentId)) {
      throw new NotFoundException("Unknown job: " + jobId);
    }
    return jobEventRepository.findByJobId(jobId);
  }

  public ReplayCopySearchExportResponse getSearchExport(String environmentId, String jobId) {
    requireEnvironment(environmentId);
    if (isMockMode()) {
      ReplayCopyJobStatusResponse job = mockJobs.get(jobId);
      if (job == null || !job.environmentId().equals(environmentId)) {
        throw new NotFoundException("Unknown job: " + jobId);
      }
      if (job.jobType() != JobType.SEARCH) {
        throw new BadRequestException("Search export is only available for SEARCH jobs.");
      }
      return new ReplayCopySearchExportResponse(
          environmentId,
          jobId,
          job.topicName(),
          job.searchMatchedMessages(),
          job.searchExportFileName() == null ? "search-results-" + jobId + ".json" : job.searchExportFileName(),
          "application/json",
          "[]",
          Instant.now(),
          "Search results export is ready.");
    }

    JobRecord job = jobRepository.findById(jobId)
        .orElseThrow(() -> new NotFoundException("Unknown job: " + jobId));
    if (!job.environmentId().equals(environmentId)) {
      throw new NotFoundException("Unknown job: " + jobId);
    }
    if (job.type() != JobType.SEARCH) {
      throw new BadRequestException("Search export is only available for SEARCH jobs.");
    }

    Map<String, Object> parameters = job.parameters();
    boolean ready = booleanValue(parameters.get("searchExportReady"));
    if (!ready) {
      throw new BadRequestException("Search export is not ready yet for job: " + jobId);
    }

    List<ReplayCopyJobSearchResultRepository.ReplayCopySearchResultRecord> matches =
        replayCopyJobSearchResultRepository.findByJobId(jobId);
    String fileName = stringValue(parameters.get("searchExportFileName"));
    if (fileName == null || fileName.isBlank()) {
      fileName = "search-results-" + jobId + ".json";
    }
    String content = buildSearchExportJson(matches);
    return new ReplayCopySearchExportResponse(
        environmentId,
        jobId,
        stringValue(parameters.get("topicName")),
        matches.size(),
        fileName,
        "application/json",
        content,
        Instant.now(),
        "Search results export is ready.");
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
    if (request.messageLimit() < 1 || request.messageLimit() > 1_000_000) {
      throw new BadRequestException("Message limit must be between 1 and 1000000.");
    }

    if (request.messagesPerSecond() < 1 || request.messagesPerSecond() > 5000) {
      throw new BadRequestException("Rate limit must be between 1 and 5000 messages per second.");
    }

    String operationMode = normalizeOperationMode(request);
    if ("ACK_AND_MOVE".equals(operationMode)
        && request.topicName() != null
        && request.destinationTopicName() != null
        && request.topicName().equals(request.destinationTopicName())) {
      throw new BadRequestException("Destination topic must be different from the source topic.");
    }
  }

  private String normalizeOperationMode(ReplayCopyJobRequest request) {
    String explicit = blankToNull(request.operationMode());
    if (explicit != null) {
      String normalized = explicit.toUpperCase();
      if (!normalized.equals("ACK_ONLY")
          && !normalized.equals("ACK_AND_MOVE")
          && !normalized.equals("SEARCH_ONLY")) {
        throw new BadRequestException("operationMode must be ACK_ONLY, ACK_AND_MOVE, or SEARCH_ONLY.");
      }
      return normalized;
    }

    String legacy = blankToNull(request.operation());
    if (legacy == null) {
      throw new BadRequestException("Either operationMode or operation is required.");
    }
    return switch (legacy.toUpperCase()) {
      case "REPLAY" -> "ACK_ONLY";
      case "COPY" -> "ACK_AND_MOVE";
      default -> throw new BadRequestException("Operation must be REPLAY or COPY.");
    };
  }

  private JobType toJobType(String operationMode) {
    return switch (operationMode) {
      case "ACK_ONLY" -> JobType.REPLAY;
      case "ACK_AND_MOVE" -> JobType.COPY;
      case "SEARCH_ONLY" -> JobType.SEARCH;
      default -> throw new BadRequestException("Unsupported operation mode: " + operationMode);
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
        booleanValue(parameters.get("autoReplicateSchema")),
        intValue(parameters.get("scannedMessages")),
        intValue(parameters.get("matchedMessages")),
        intValue(parameters.get("nonMatchedMessages")),
        intValue(parameters.get("ackedMessages")),
        intValue(parameters.get("nackedMessages")),
        intValue(parameters.get("movedMessages")),
        intValue(parameters.get("failedMessages")),
        intValue(parameters.get("publishedMessages")),
        intValue(parameters.get("searchMatchedMessages")),
        stringValue(parameters.get("searchExportId")),
        booleanValue(parameters.get("searchExportReady")),
        stringValue(parameters.get("searchExportFileName")),
        intValue(parameters.get("progressPercent")),
        doubleValue(parameters.get("messagesPerSecondActual")),
        longValue(parameters.get("estimatedRemainingSeconds")),
        instantValue(parameters.get("startedAt")),
        instantValue(parameters.get("completedAt")),
        stringValue(parameters.get("lastMessageId")),
        stringValue(parameters.get("lastError")),
        stringValue(parameters.get("statusMessage")),
        stringListValue(parameters.get("warnings")),
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

  private boolean booleanValue(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value == null) {
      return false;
    }
    return Boolean.parseBoolean(value.toString());
  }

  private double doubleValue(Object value) {
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    if (value == null) {
      return 0d;
    }
    return Double.parseDouble(value.toString());
  }

  private long longValue(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value == null) {
      return 0L;
    }
    return Long.parseLong(value.toString());
  }

  private Instant instantValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant instant) {
      return instant;
    }
    String text = value.toString();
    if (text.isBlank() || "null".equalsIgnoreCase(text)) {
      return null;
    }
    return Instant.parse(text);
  }

  private List<String> validateSchemaCompatibility(
      String environmentId,
      TopicDetails sourceTopic,
      String destinationTopicName,
      JobType jobType,
      String operationMode) {
    if (jobType == JobType.SEARCH) {
      return List.of("Search mode scans and collects matched messages only. It does not ACK, NACK, or publish.");
    }
    if (jobType != JobType.COPY) {
      return List.of("Replay mode will ACK matched messages and NACK non-matches without publishing to a destination topic.");
    }
    if (destinationTopicName == null || destinationTopicName.isBlank()) {
      throw new BadRequestException("Destination topic name is required for COPY operation.");
    }
    TopicDetails destinationTopic = findTopic(environmentId, destinationTopicName);
    if (destinationTopic == null) {
      return List.of("Destination topic is not present in the current synced snapshot, so schema compatibility could not be verified.");
    }

    SchemaSummary sourceSchema = sourceTopic.schema();
    SchemaSummary destinationSchema = destinationTopic.schema();

    if (!sourceSchema.present() || !destinationSchema.present()) {
      return List.of("One or both topics do not expose schema metadata, so compatibility could not be fully verified before submit.");
    }

    if (!sourceSchema.type().equalsIgnoreCase(destinationSchema.type())) {
      throw new BadRequestException(
          "Source topic schema type " + sourceSchema.type()
              + " does not match destination schema type " + destinationSchema.type() + ".");
    }

    if (!sourceSchema.compatibility().equalsIgnoreCase(destinationSchema.compatibility())) {
      return List.of(
          "Source schema compatibility is " + sourceSchema.compatibility()
              + " while destination compatibility is " + destinationSchema.compatibility()
              + ". Review the destination policy before replaying.");
    }

    return List.of("Schema types match between source and destination topics.");
  }

  private String normalizeDestinationTopic(JobType jobType, String destinationTopicName) {
    if (jobType == JobType.COPY) {
      String normalized = blankToNull(destinationTopicName);
      if (normalized == null) {
        throw new BadRequestException("Destination topic name is required for COPY operation.");
      }
      return normalized;
    }
    return blankToNull(destinationTopicName);
  }

  private List<Map<String, String>> normalizeCriteriaRows(List<Map<String, String>> rows) {
    if (rows == null || rows.isEmpty()) {
      return List.of();
    }

    List<Map<String, String>> normalizedRows = new ArrayList<>();
    Set<String> dedupe = new HashSet<>();
    for (Map<String, String> row : rows) {
      if (row == null || row.isEmpty()) {
        continue;
      }
      Map<String, String> normalizedRow = new LinkedHashMap<>();
      row.forEach((field, value) -> {
        String normalizedField = blankToNull(field);
        String normalizedValue = blankToNull(value);
        if (normalizedField != null && normalizedValue != null) {
          normalizedRow.put(normalizedField, normalizedValue);
        }
      });
      if (normalizedRow.isEmpty()) {
        continue;
      }
      String signature = normalizedRow.toString();
      if (dedupe.add(signature)) {
        normalizedRows.add(normalizedRow);
      }
    }
    return normalizedRows;
  }

  private TopicDetails findTopic(String environmentId, String topicName) {
    try {
      return requireTopic(environmentId, topicName);
    } catch (NotFoundException exception) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> stringListValue(Object value) {
    if (value instanceof List<?> list) {
      return list.stream().filter(item -> item != null).map(Object::toString).toList();
    }
    return List.of();
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

  private String buildSearchExportJson(
      List<ReplayCopyJobSearchResultRepository.ReplayCopySearchResultRecord> matches) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    for (int index = 0; index < matches.size(); index++) {
      ReplayCopyJobSearchResultRepository.ReplayCopySearchResultRecord record = matches.get(index);
      if (index > 0) {
        builder.append(",");
      }
      builder.append("{");
      builder.append("\"id\":").append(record.id()).append(",");
      builder.append("\"messageId\":").append(toJsonString(record.messageId())).append(",");
      builder.append("\"messageKey\":").append(toJsonString(record.messageKey())).append(",");
      builder.append("\"matchedFieldValue\":").append(toJsonString(record.matchedFieldValue())).append(",");
      builder.append("\"properties\":").append(toJsonMap(record.properties())).append(",");
      builder.append("\"payload\":").append(toJsonString(record.payload()));
      builder.append("}");
    }
    builder.append("]");
    return builder.toString();
  }

  private String toJsonMap(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return "{}";
    }
    StringBuilder builder = new StringBuilder("{");
    int index = 0;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (index++ > 0) {
        builder.append(",");
      }
      builder.append(toJsonString(entry.getKey())).append(":").append(toJsonString(entry.getValue()));
    }
    builder.append("}");
    return builder.toString();
  }

  private String toJsonString(String value) {
    if (value == null) {
      return "null";
    }
    return "\"" + value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t") + "\"";
  }
}
