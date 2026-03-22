package com.pulsaradmin.api.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "app.pulsar.gateway-mode=mock")
@AutoConfigureMockMvc
class CatalogControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldReturnCatalogSummary() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/catalog"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.tenants[0].name").exists())
        .andExpect(jsonPath("$.namespaces[0].tenant").exists());
  }

  @Test
  void shouldCreateTenant() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/tenants")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "finance",
                  "adminRoles": ["platform-admin"],
                  "allowedClusters": ["prod-east"]
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.resourceType").value("TENANT"))
        .andExpect(jsonPath("$.resourceName").value("finance"))
        .andExpect(jsonPath("$.catalogSummary.tenants[?(@.name=='finance')]").exists());
  }

  @Test
  void shouldCreateNamespace() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "sandbox"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.resourceType").value("NAMESPACE"))
        .andExpect(jsonPath("$.resourceName").value("acme/sandbox"))
        .andExpect(jsonPath("$.catalogSummary.namespaces[?(@.tenant=='acme' && @.namespace=='sandbox')]").exists());
  }

  @Test
  void shouldReturnNamespaceDetails() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/namespaces/detail")
            .param("tenant", "acme")
            .param("namespace", "orders"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.namespace").value("orders"))
        .andExpect(jsonPath("$.policies.messageTtlInSeconds").exists());
  }

  @Test
  void shouldUpdateNamespacePolicies() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces/policies")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "orders",
                  "policies": {
                    "retentionTimeInMinutes": 2880,
                    "retentionSizeInMb": 2048,
                    "messageTtlInSeconds": 7200,
                    "deduplicationEnabled": true,
                    "backlogQuotaLimitInBytes": 1073741824,
                    "backlogQuotaLimitTimeInSeconds": 3600,
                    "dispatchRatePerTopicInMsg": 2500,
                    "dispatchRatePerTopicInByte": 1048576,
                    "publishRateInMsg": 2500,
                    "publishRateInByte": 1048576
                  },
                  "reason": "Align namespace defaults for operator testing"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.namespaceDetails.policies.deduplicationEnabled").value(true));
  }

  @Test
  void shouldPreviewNamespaceYaml() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces/yaml/preview")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "orders",
                  "yaml": "tenant: acme\\nnamespace: orders\\npolicies:\\n  retentionTimeInMinutes: 1440\\n  retentionSizeInMb: 0\\n  messageTtlInSeconds: 0\\n  deduplicationEnabled: false\\n  backlogQuotaLimitInBytes: 0\\n  backlogQuotaLimitTimeInSeconds: 0\\n  dispatchRatePerTopicInMsg: 0\\n  dispatchRatePerTopicInByte: 0\\n  publishRateInMsg: 0\\n  publishRateInByte: 0\\ntopics:\\n  - name: order-events\\n    domain: persistent\\n    partitions: 0\\n    notes: \\"Healthy live traffic with steady subscriber throughput.\\"\\n    policies:\\n      retentionTimeInMinutes: 0\\n      retentionSizeInMb: 0\\n      ttlInSeconds: 0\\n      compactionThresholdInBytes: 0\\n      maxProducers: 10\\n      maxConsumers: 0\\n      maxSubscriptions: 0\\n  - name: payment-events\\n    domain: persistent\\n    partitions: 0\\n    notes: \\"Backlog-heavy topic with slow consumer dispatch and high oldest-message age.\\"\\n    policies:\\n      retentionTimeInMinutes: 0\\n      retentionSizeInMb: 0\\n      ttlInSeconds: 0\\n      compactionThresholdInBytes: 0\\n      maxProducers: 0\\n      maxConsumers: 0\\n      maxSubscriptions: 0\\n"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.namespace").value("orders"))
        .andExpect(jsonPath("$.previewId").exists())
        .andExpect(jsonPath("$.changes").isArray());
  }

  @Test
  void shouldRequireConfirmationForDangerousTopicRemovalInYamlPreview() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces/yaml/preview")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "orders",
                  "yaml": "tenant: acme\\nnamespace: orders\\npolicies:\\n  retentionTimeInMinutes: 0\\n  retentionSizeInMb: 0\\n  messageTtlInSeconds: 0\\n  deduplicationEnabled: false\\n  backlogQuotaLimitInBytes: 0\\n  backlogQuotaLimitTimeInSeconds: 0\\n  dispatchRatePerTopicInMsg: 0\\n  dispatchRatePerTopicInByte: 0\\n  publishRateInMsg: 0\\n  publishRateInByte: 0\\ntopics: []\\n"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.dangerousRemovals").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
        .andExpect(jsonPath("$.blockedChanges").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
        .andExpect(jsonPath("$.requiredConfirmations[?(@=='remove-topic:persistent://acme/orders/order-events')]").exists())
        .andExpect(jsonPath("$.changes[?(@.resourceName=='persistent://acme/orders/order-events')].action").value(org.hamcrest.Matchers.hasItem("REMOVE")))
        .andExpect(jsonPath("$.changes[?(@.resourceName=='persistent://acme/orders/order-events')].resourceType").value(org.hamcrest.Matchers.hasItem("TOPIC")))
        .andExpect(jsonPath("$.changes[?(@.resourceName=='persistent://acme/orders/order-events')].requiresConfirmation").value(org.hamcrest.Matchers.hasItem(true)));
  }

  @Test
  void shouldRejectYamlApplyWithoutDangerousRemovalConfirmation() throws Exception {
    String previewBody = mockMvc.perform(post("/api/v1/environments/prod/namespaces/yaml/preview")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "orders",
                  "yaml": "tenant: acme\\nnamespace: orders\\npolicies:\\n  retentionTimeInMinutes: 0\\n  retentionSizeInMb: 0\\n  messageTtlInSeconds: 0\\n  deduplicationEnabled: false\\n  backlogQuotaLimitInBytes: 0\\n  backlogQuotaLimitTimeInSeconds: 0\\n  dispatchRatePerTopicInMsg: 0\\n  dispatchRatePerTopicInByte: 0\\n  publishRateInMsg: 0\\n  publishRateInByte: 0\\ntopics: []\\n"
                }
                """))
        .andExpect(status().isOk())
        .andReturn()
        .getResponse()
        .getContentAsString();

    String previewId = previewBody.replaceAll(".*\\\"previewId\\\":\\\"([^\\\"]+)\\\".*", "$1");

    mockMvc.perform(post("/api/v1/environments/prod/namespaces/yaml/apply")
            .contentType("application/json")
            .content("""
                {
                  "previewId": "%s",
                  "reason": "Remove topic through namespace YAML",
                  "confirmedChangeKeys": []
                }
                """.formatted(previewId)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Confirm the destructive YAML removals")));
  }

  @Test
  void shouldPreviewPartitionIncreaseForExistingPartitionedTopic() throws Exception {
    String currentYaml = mockMvc.perform(get("/api/v1/environments/prod/namespaces/yaml/current")
            .param("tenant", "acme")
            .param("namespace", "analytics"))
        .andExpect(status().isOk())
        .andReturn()
        .getResponse()
        .getContentAsString();

    String yamlBody = currentYaml.replaceAll(".*\\\"yaml\\\":\\\"(.*)\\\",\\\"message\\\".*", "$1")
        .replace("\\n", "\n")
        .replace("\\\"", "\"")
        .replace("\\\\", "\\");
    String updatedYaml = yamlBody.replace("partitions: 6", "partitions: 8");

    mockMvc.perform(post("/api/v1/environments/prod/namespaces/yaml/preview")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "analytics",
                  "yaml": %s
                }
                """.formatted(toJsonString(updatedYaml))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.valid").value(true))
        .andExpect(jsonPath("$.totalUpdates").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
        .andExpect(jsonPath("$.changes[?(@.resourceType=='TOPIC')].resourceName").value(org.hamcrest.Matchers.hasItem("persistent://acme/analytics/usage-rollups")))
        .andExpect(jsonPath("$.changes[?(@.resourceType=='TOPIC')].summary").value(org.hamcrest.Matchers.hasItem("Topic lifecycle settings will be aligned to the YAML state.")));
  }

  private static String toJsonString(String value) {
    return "\"" + value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        + "\"";
  }
}
