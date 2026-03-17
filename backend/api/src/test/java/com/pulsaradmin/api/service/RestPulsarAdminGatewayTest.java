package com.pulsaradmin.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import org.apache.pulsar.client.api.AuthenticationFactory;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class RestPulsarAdminGatewayTest {
  @Test
  void shouldEnrichSyncedTopicsWithStatsSubscriptionsAndSchema() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    EnvironmentDetails environment = new EnvironmentDetails(
        "prod",
        "Production",
        "prod",
        EnvironmentStatus.HEALTHY,
        "us-east-1",
        "prod-east",
        "Primary production cluster",
        "pulsar+ssl://prod-brokers:6651",
        "https://pulsar-admin.prod.example.com",
        "none",
        null,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        Instant.parse("2026-03-17T18:00:00Z"),
        "SUCCESS",
        "Connection verified.",
        Instant.parse("2026-03-17T17:59:00Z"),
        false);

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/tenants"))
        .andRespond(withSuccess("""
            ["acme"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/namespaces/acme"))
        .andRespond(withSuccess("""
            ["acme/orders"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders"))
        .andRespond(withSuccess("""
            ["persistent://acme/orders/payment-events"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/stats"))
        .andRespond(withSuccess("""
            {
              "msgRateIn": 82.4,
              "msgRateOut": 64.7,
              "msgThroughputIn": 30244.0,
              "msgThroughputOut": 21910.0,
              "storageSize": 884736,
              "publishers": [
                {"producerName": "payments-producer-1"},
                {"producerName": "payments-producer-2"}
              ],
              "subscriptions": {
                "payment-settlement": {
                  "msgBacklog": 1200,
                  "consumers": [
                    {"consumerName": "settlement-a"}
                  ]
                },
                "payment-audit": {
                  "msgBacklog": 25,
                  "consumers": [
                    {"consumerName": "audit-a"},
                    {"consumerName": "audit-b"}
                  ]
                }
              },
              "partitions": {
                "persistent://acme/orders/payment-events-partition-0": {
                  "msgRateIn": 44.2,
                  "msgRateOut": 32.0,
                  "subscriptions": {
                    "payment-settlement": {
                      "msgBacklog": 600,
                      "consumers": [{"consumerName": "settlement-a"}]
                    }
                  }
                },
                "persistent://acme/orders/payment-events-partition-1": {
                  "msgRateIn": 38.2,
                  "msgRateOut": 32.7,
                  "subscriptions": {
                    "payment-settlement": {
                      "msgBacklog": 625,
                      "consumers": [{"consumerName": "settlement-b"}]
                    }
                  }
                }
              }
            }
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/schemas/acme/orders/payment-events/schema"))
        .andRespond(withSuccess("""
            {
              "version": "12",
              "type": "JSON",
              "data": {
                "type": "JSON"
              }
            }
            """, MediaType.APPLICATION_JSON));

    EnvironmentSnapshot snapshot = gateway.syncMetadata(environment);

    assertThat(snapshot.tenants()).containsExactly("acme");
    assertThat(snapshot.namespaces()).containsExactly("acme/orders");
    assertThat(snapshot.topics()).hasSize(1);

    var topic = snapshot.topics().get(0);
    assertThat(topic.fullName()).isEqualTo("persistent://acme/orders/payment-events");
    assertThat(topic.stats().backlog()).isEqualTo(1225);
    assertThat(topic.stats().producers()).isEqualTo(2);
    assertThat(topic.stats().subscriptions()).isEqualTo(2);
    assertThat(topic.stats().consumers()).isEqualTo(3);
    assertThat(topic.subscriptions()).containsExactly("payment-audit", "payment-settlement");
    assertThat(topic.partitioned()).isTrue();
    assertThat(topic.partitionSummaries()).hasSize(2);
    assertThat(topic.schema().present()).isTrue();
    assertThat(topic.schema().type()).isEqualTo("JSON");
    assertThat(topic.notes()).contains("Found 2 subscriptions");

    server.verify();
  }

  @Test
  void shouldPeekMessagesThroughPulsarClient() throws Exception {
    PulsarClient client = mock(PulsarClient.class);
    @SuppressWarnings("unchecked")
    ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
    @SuppressWarnings("unchecked")
    Reader<byte[]> reader = mock(Reader.class);
    @SuppressWarnings("unchecked")
    Message<byte[]> firstMessage = mock(Message.class);
    @SuppressWarnings("unchecked")
    Message<byte[]> secondMessage = mock(Message.class);

    when(client.getPartitionsForTopic("persistent://acme/orders/payment-events"))
        .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));
    when(client.newReader()).thenReturn(readerBuilder);
    when(readerBuilder.topic("persistent://acme/orders/payment-events")).thenReturn(readerBuilder);
    when(readerBuilder.startMessageFromRollbackDuration(3650, java.util.concurrent.TimeUnit.DAYS)).thenReturn(readerBuilder);
    when(readerBuilder.create()).thenReturn(reader);

    when(reader.readNext(250, java.util.concurrent.TimeUnit.MILLISECONDS))
        .thenReturn(firstMessage, secondMessage, null);

    when(firstMessage.getMessageId()).thenReturn(mock(org.apache.pulsar.client.api.MessageId.class));
    when(firstMessage.getMessageId().toString()).thenReturn("ledger:91:2048");
    when(firstMessage.hasKey()).thenReturn(true);
    when(firstMessage.getKey()).thenReturn("payment-10412");
    when(firstMessage.getPublishTime()).thenReturn(1_742_240_000_000L);
    when(firstMessage.getEventTime()).thenReturn(1_742_239_999_000L);
    when(firstMessage.getProducerName()).thenReturn("payments-producer-1");
    when(firstMessage.getData()).thenReturn("""
        {"paymentId":"10412","state":"AUTHORIZED"}
        """.getBytes());
    when(firstMessage.getSchemaVersion()).thenReturn(new byte[] {0x0c});

    when(secondMessage.getMessageId()).thenReturn(mock(org.apache.pulsar.client.api.MessageId.class));
    when(secondMessage.getMessageId().toString()).thenReturn("ledger:91:2049");
    when(secondMessage.hasKey()).thenReturn(false);
    when(secondMessage.getPublishTime()).thenReturn(1_742_240_010_000L);
    when(secondMessage.getEventTime()).thenReturn(0L);
    when(secondMessage.getProducerName()).thenReturn("");
    when(secondMessage.getData()).thenReturn("""
        {"paymentId":"10413","state":"SETTLEMENT_PENDING"}
        """.getBytes());
    when(secondMessage.getSchemaVersion()).thenReturn(null);

    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(
        RestClient.builder().build(),
        new ObjectMapper(),
        environment -> client);

    PeekMessagesResponse response = gateway.peekMessages(environment(), "persistent://acme/orders/payment-events", 2);

    assertThat(response.environmentId()).isEqualTo("prod");
    assertThat(response.returnedCount()).isEqualTo(2);
    assertThat(response.messages()).hasSize(2);
    assertThat(response.messages().get(0).key()).isEqualTo("payment-10412");
    assertThat(response.messages().get(0).schemaVersion()).isEqualTo("0c");
    assertThat(response.messages().get(1).producerName()).isEqualTo("Unknown producer");
    assertThat(response.messages().get(1).schemaVersion()).isEqualTo("unknown");

    verify(reader).close();
  }

  @Test
  void shouldSendBearerHeaderForTokenProtectedAdminRequests() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/clusters"))
        .andExpect(header("Authorization", "Bearer secret-token-value"))
        .andRespond(withSuccess("""
            ["prod-a"]
            """, MediaType.APPLICATION_JSON));

    var result = gateway.testConnection(tokenEnvironment("token:secret-token-value"));

    assertThat(result.successful()).isTrue();
    server.verify();
  }

  @Test
  void shouldRejectUnsupportedVaultCredentialReference() {
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(RestClient.builder().build(), new ObjectMapper());

    assertThatThrownBy(() -> gateway.testConnection(tokenEnvironment("vault://pulsar/prod")))
        .hasMessageContaining("Vault-style credential references are not wired into this prototype yet");
  }

  private EnvironmentDetails environment() {
    return new EnvironmentDetails(
        "prod",
        "Production",
        "prod",
        EnvironmentStatus.HEALTHY,
        "us-east-1",
        "prod-east",
        "Primary production cluster",
        "pulsar+ssl://prod-brokers:6651",
        "https://pulsar-admin.prod.example.com",
        "none",
        null,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        Instant.parse("2026-03-17T18:00:00Z"),
        "SUCCESS",
        "Connection verified.",
        Instant.parse("2026-03-17T17:59:00Z"),
        false);
  }

  private EnvironmentDetails tokenEnvironment(String credentialReference) {
    return new EnvironmentDetails(
        "prod",
        "Production",
        "prod",
        EnvironmentStatus.HEALTHY,
        "us-east-1",
        "prod-east",
        "Primary production cluster",
        "pulsar+ssl://prod-brokers:6651",
        "https://pulsar-admin.prod.example.com",
        "token",
        credentialReference,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        Instant.parse("2026-03-17T18:00:00Z"),
        "SUCCESS",
        "Connection verified.",
        Instant.parse("2026-03-17T17:59:00Z"),
        false);
  }
}
