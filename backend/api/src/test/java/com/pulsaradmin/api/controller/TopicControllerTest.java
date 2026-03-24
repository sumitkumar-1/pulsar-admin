package com.pulsaradmin.api.controller;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;

@SpringBootTest(properties = "app.pulsar.gateway-mode=mock")
@AutoConfigureMockMvc
class TopicControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldFilterTopicsBySearchAndPaginate() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics")
            .param("search", "payment")
            .param("page", "0")
            .param("pageSize", "25"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items", hasSize(1)))
        .andExpect(jsonPath("$.items[0].topic").value("payment-events"))
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  void shouldReturnTopicDetails() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/detail")
            .param("topic", "persistent://acme/orders/payment-events"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.fullName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.stats.backlog", greaterThan(1000)))
        .andExpect(jsonPath("$.subscriptions", hasSize(2)));
  }

  @Test
  @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
  void shouldCreateTopic() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics")
            .contentType("application/json")
            .content("""
                {
                  "domain": "persistent",
                  "tenant": "acme",
                  "namespace": "orders",
                  "topic": "payment-events-retry",
                  "partitions": 0,
                  "notes": "Create a bounded retry topic for payment incident replay"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.fullName").value("persistent://acme/orders/payment-events-retry"))
        .andExpect(jsonPath("$.topic").value("payment-events-retry"))
        .andExpect(jsonPath("$.partitioned").value(false));
  }

  @Test
  void shouldCreateSubscription() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/subscriptions")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-incident-review",
                  "initialPosition": "EARLIEST",
                  "reason": "Create a fresh subscription for replay verification"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.action").value("CREATE"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-incident-review"))
        .andExpect(jsonPath("$.initialPosition").value("EARLIEST"))
        .andExpect(jsonPath("$.topicDetails.subscriptions", hasSize(3)));
  }

  @Test
  void shouldDeleteSubscription() throws Exception {
    mockMvc.perform(delete("/api/v1/environments/prod/topics/subscriptions")
            .param("topic", "persistent://acme/orders/payment-events")
            .param("subscription", "payment-alerts"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.action").value("DELETE"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-alerts"))
        .andExpect(jsonPath("$.topicDetails.subscriptions", hasSize(1)));
  }

  @Test
  void shouldReturnPeekMessages() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/peek")
            .param("topic", "persistent://acme/orders/payment-events")
            .param("limit", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.returnedCount").value(2))
        .andExpect(jsonPath("$.messages", hasSize(2)))
        .andExpect(jsonPath("$.messages[0].messageId").exists());
  }

  @Test
  void shouldTerminateTopic() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/terminate")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "reason": "Freeze the stream after incident replay closes out"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldRejectTerminateForPartitionedTopic() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/terminate")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/analytics/usage-rollups",
                  "reason": "Attempting an unsupported operation for regression coverage"
                }
                """))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Termination is not supported for partitioned topics. Select a non-partitioned topic instead."));
  }

  @Test
  void shouldReturnTopicPolicies() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/policies")
            .param("topic", "persistent://acme/orders/payment-events"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.policies.maxSubscriptions").exists());
  }

  @Test
  void shouldUpdateTopicPolicies() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/policies")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "policies": {
                    "retentionTimeInMinutes": 2880,
                    "retentionSizeInMb": 1024,
                    "ttlInSeconds": 7200,
                    "compactionThresholdInBytes": 67108864,
                    "maxProducers": 12,
                    "maxConsumers": 32,
                    "maxSubscriptions": 16
                  },
                  "reason": "Tighten topic policies for incident testing"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.policies.maxProducers").value(12));
  }

  @Test
  void shouldPublishTestMessage() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/publish")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "key": "payment-10412",
                  "properties": {"origin":"ui-test"},
                  "schemaMode": "JSON",
                  "payload": "{\\\"paymentId\\\":\\\"10412\\\",\\\"state\\\":\\\"TEST\\\"}",
                  "reason": "Publish a bounded smoke-test event"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.messageId").exists())
        .andExpect(jsonPath("$.warnings").isArray());
  }

  @Test
  void shouldConsumeTestMessages() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/consume")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": null,
                  "ephemeral": true,
                  "maxMessages": 2,
                  "waitTimeSeconds": 3,
                  "reason": "Read a bounded preview for verification"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.requestedCount").value(2))
        .andExpect(jsonPath("$.messages").isArray())
        .andExpect(jsonPath("$.warnings").isArray());
  }

  @Test
  void shouldExportBoundedPeekMessages() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/export")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "source": "PEEK",
                  "subscriptionName": null,
                  "ephemeral": true,
                  "maxMessages": 2,
                  "waitTimeSeconds": 5,
                  "reason": "Export bounded sampled messages for debugging"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.source").value("PEEK"))
        .andExpect(jsonPath("$.fileName").value("acme-orders-payment-events-peek-export.json"))
        .andExpect(jsonPath("$.content").exists());
  }

  @Test
  void shouldResetCursor() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/reset-cursor")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "target": "LATEST",
                  "timestamp": null,
                  "reason": "Clear backlog after incident validation"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-settlement"))
        .andExpect(jsonPath("$.target").value("LATEST"))
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldSkipMessages() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/skip-messages")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "messageCount": 25,
                  "reason": "Skip poison messages after alert triage"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-settlement"))
        .andExpect(jsonPath("$.skippedMessages").value(25))
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldUnloadTopic() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/unload")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "reason": "Force broker ownership refresh after incident cleanup"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.message").exists())
        .andExpect(jsonPath("$.topicDetails.fullName").value("persistent://acme/orders/payment-events"));
  }

  @Test
  void shouldCreateReplayCopyJob() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/replay-copy")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "operation": "COPY",
                  "destinationTopicName": "persistent://acme/dev/replay-lab",
                  "messageLimit": 120,
                  "filterText": "payment-10412",
                  "messagesPerSecond": 50,
                  "reason": "Copy incident-related messages into replay lab"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").exists())
        .andExpect(jsonPath("$.jobType").value("COPY"))
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.status").value("COMPLETED"))
        .andExpect(jsonPath("$.destinationTopicName").value("persistent://acme/dev/replay-lab"));
  }

  @Test
  void shouldCreateReplayCopyJobFromMultipart() throws Exception {
    MockMultipartFile requestPart = new MockMultipartFile(
        "request",
        "",
        "application/json",
        """
            {
              "topicName": "persistent://acme/orders/payment-events",
              "subscriptionName": "payment-settlement",
              "operation": "COPY",
              "destinationTopicName": "persistent://acme/dev/replay-lab",
              "matchField": "feedId",
              "messageLimit": 120,
              "messagesPerSecond": 50,
              "reason": "Copy incident-related messages into replay lab"
            }
            """.getBytes());
    MockMultipartFile idsFile = new MockMultipartFile(
        "idsFile",
        "ids.csv",
        "text/csv",
        "feedId\n10412\n10413\n".getBytes());

    mockMvc.perform(multipart("/api/v1/environments/prod/topics/replay-copy")
            .file(requestPart)
            .file(idsFile))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").exists())
        .andExpect(jsonPath("$.jobType").value("COPY"))
        .andExpect(jsonPath("$.matchField").value("feedId"));
  }

  @Test
  void shouldGetReplayCopyJobStatus() throws Exception {
    String response = mockMvc.perform(post("/api/v1/environments/prod/topics/replay-copy")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "operation": "REPLAY",
                  "destinationTopicName": "persistent://acme/dev/replay-lab",
                  "messageLimit": 75,
                  "filterText": "",
                  "messagesPerSecond": 25,
                  "reason": "Replay a bounded slice for verification"
                }
                """))
        .andExpect(status().isOk())
        .andReturn()
        .getResponse()
        .getContentAsString();

    String jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(response).get("jobId").asText();

    mockMvc.perform(get("/api/v1/environments/prod/topics/jobs/{jobId}", jobId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").value(jobId))
        .andExpect(jsonPath("$.status").value("COMPLETED"))
        .andExpect(jsonPath("$.publishedMessages").value(0))
        .andExpect(jsonPath("$.ackedMessages").value(24))
        .andExpect(jsonPath("$.nackedMessages").value(24));
  }

  @Test
  void shouldGetReplayCopyJobEvents() throws Exception {
    String response = mockMvc.perform(post("/api/v1/environments/prod/topics/replay-copy")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "operation": "COPY",
                  "destinationTopicName": "persistent://acme/dev/replay-lab",
                  "messageLimit": 25,
                  "filterText": "",
                  "messagesPerSecond": 25,
                  "reason": "Event timeline smoke test"
                }
                """))
        .andExpect(status().isOk())
        .andReturn()
        .getResponse()
        .getContentAsString();

    String jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(response).get("jobId").asText();

    mockMvc.perform(get("/api/v1/environments/prod/topics/jobs/{jobId}/events", jobId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].jobId").value(jobId))
        .andExpect(jsonPath("$[0].eventType").exists());
  }

  @Test
  void shouldCreateSearchOnlyJobAndReturnSearchExport() throws Exception {
    String response = mockMvc.perform(post("/api/v1/environments/prod/topics/replay-copy")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "operationMode": "SEARCH_ONLY",
                  "matchField": "feedId",
                  "messageLimit": 25,
                  "messagesPerSecond": 25,
                  "reason": "Search-only smoke test"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobType").value("SEARCH"))
        .andExpect(jsonPath("$.searchExportReady").value(true))
        .andReturn()
        .getResponse()
        .getContentAsString();

    String jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(response).get("jobId").asText();

    mockMvc.perform(get("/api/v1/environments/prod/topics/jobs/{jobId}/search-export", jobId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").value(jobId))
        .andExpect(jsonPath("$.contentType").value("application/json"))
        .andExpect(jsonPath("$.fileName").exists());
  }

  @Test
  void shouldRejectInvalidPageSize() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics").param("pageSize", "7"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldReturnNotFoundForUnknownTopic() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/detail")
            .param("topic", "persistent://acme/orders/missing"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message").value("Unknown topic: persistent://acme/orders/missing"));
  }

  @Test
  void shouldRejectCreateTopicWhenTopicAlreadyExists() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics")
            .contentType("application/json")
            .content("""
                {
                  "domain": "persistent",
                  "tenant": "acme",
                  "namespace": "orders",
                  "topic": "payment-events",
                  "partitions": 0,
                  "notes": "Duplicate topic"
                }
                """))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Topic already exists: persistent://acme/orders/payment-events"));
  }
}
