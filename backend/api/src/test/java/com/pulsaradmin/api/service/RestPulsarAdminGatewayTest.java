package com.pulsaradmin.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class RestPulsarAdminGatewayTest {
  @Test
  void shouldBuildInventoryFirstSnapshotWithLazyTopicEnrichment() {
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
        null,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        null,
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
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/partitioned"))
        .andRespond(withSuccess("""
            ["persistent://acme/orders/payment-events"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/partitions"))
        .andRespond(withSuccess("""
            {"partitions": 2}
            """, MediaType.APPLICATION_JSON));
    EnvironmentSnapshot snapshot = gateway.syncMetadata(environment);

    assertThat(snapshot.tenants()).containsExactly("acme");
    assertThat(snapshot.namespaces()).containsExactly("acme/orders");
    assertThat(snapshot.topics()).hasSize(1);

    var topic = snapshot.topics().get(0);
    assertThat(topic.fullName()).isEqualTo("persistent://acme/orders/payment-events");
    assertThat(topic.stats().backlog()).isZero();
    assertThat(topic.stats().producers()).isZero();
    assertThat(topic.stats().subscriptions()).isZero();
    assertThat(topic.stats().consumers()).isZero();
    assertThat(topic.subscriptions()).isEmpty();
    assertThat(topic.partitioned()).isTrue();
    assertThat(topic.partitionSummaries()).hasSize(2);
    assertThat(topic.schema().present()).isFalse();
    assertThat(topic.schema().type()).isEqualTo("UNKNOWN");
    assertThat(topic.notes()).contains("Live topic stats and schema load on demand");

    server.verify();
  }

  @Test
  void shouldCollapsePartitionChildrenIntoOneLogicalTopicDuringSync() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    EnvironmentDetails environment = environment();

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
            [
              "persistent://acme/orders/payment-events-partition-0",
              "persistent://acme/orders/payment-events-partition-1",
              "persistent://acme/orders/payment-events-partition-2"
            ]
            """, MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/partitioned"))
        .andRespond(withSuccess("""
            ["persistent://acme/orders/payment-events"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/partitions"))
        .andRespond(withSuccess("""
            {"partitions": 3}
            """, MediaType.APPLICATION_JSON));
    EnvironmentSnapshot snapshot = gateway.syncMetadata(environment);

    assertThat(snapshot.topics()).hasSize(1);
    assertThat(snapshot.topics().get(0).fullName()).isEqualTo("persistent://acme/orders/payment-events");
    assertThat(snapshot.topics().get(0).partitioned()).isTrue();
    assertThat(snapshot.topics().get(0).partitions()).isEqualTo(3);
    assertThat(snapshot.topics().get(0).partitionSummaries()).hasSize(3);

    server.verify();
  }

  @Test
  void shouldFallbackToScopedNamespaceTargetsWhenTenantListIsUnauthorized() {
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
        "acme/orders",
        true,
        "SYNCED",
        "Metadata synced successfully.",
        null,
        Instant.parse("2026-03-17T18:00:00Z"),
        "SUCCESS",
        "Connection verified.",
        Instant.parse("2026-03-17T17:59:00Z"),
        false);

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/tenants"))
        .andRespond(withStatus(HttpStatus.UNAUTHORIZED).body("""
            {"reason":"not allowed"}
            """).contentType(MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders"))
        .andRespond(withSuccess("""
            ["persistent://acme/orders/payment-events"]
            """, MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/partitioned"))
        .andRespond(withSuccess("""
            []
            """, MediaType.APPLICATION_JSON));
    EnvironmentSnapshot snapshot = gateway.syncMetadata(environment);

    assertThat(snapshot.tenants()).containsExactly("acme");
    assertThat(snapshot.namespaces()).containsExactly("acme/orders");
    assertThat(snapshot.topics()).hasSize(1);
    assertThat(snapshot.health().message()).contains("using scoped targets");

    server.verify();
  }

  @Test
  void shouldExplainHowToRecoverWhenTenantListIsUnauthorizedWithoutScopedTargets() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/tenants"))
        .andRespond(withStatus(HttpStatus.UNAUTHORIZED).body("""
            {"reason":"not allowed"}
            """).contentType(MediaType.APPLICATION_JSON));

    assertThatThrownBy(() -> gateway.syncMetadata(environment()))
        .hasMessageContaining("Global tenant listing is not permitted")
        .hasMessageContaining("scoped sync targets");

    server.verify();
  }

  @Test
  void shouldBackfillMissingPartitionSummariesFromMetadataCount() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    EnvironmentDetails environment = environment();

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
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/partitioned"))
        .andRespond(withSuccess("""
            ["persistent://acme/orders/payment-events"]
            """, MediaType.APPLICATION_JSON));

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/partitions"))
        .andRespond(withSuccess("""
            {"partitions": 10}
            """, MediaType.APPLICATION_JSON));
    EnvironmentSnapshot snapshot = gateway.syncMetadata(environment);

    assertThat(snapshot.topics()).hasSize(1);
    assertThat(snapshot.topics().get(0).partitioned()).isTrue();
    assertThat(snapshot.topics().get(0).partitions()).isEqualTo(10);
    assertThat(snapshot.topics().get(0).partitionSummaries()).hasSize(10);
    assertThat(snapshot.topics().get(0).partitionSummaries().get(0).partitionName())
        .isEqualTo("persistent://acme/orders/payment-events-partition-0");
    assertThat(snapshot.topics().get(0).partitionSummaries().get(9).partitionName())
        .isEqualTo("persistent://acme/orders/payment-events-partition-9");

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
  void shouldRebuildCachedClientWhenPeekConnectionIsAlreadyClosed() throws Exception {
    PulsarClient staleClient = mock(PulsarClient.class);
    PulsarClient freshClient = mock(PulsarClient.class);
    @SuppressWarnings("unchecked")
    ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
    @SuppressWarnings("unchecked")
    Reader<byte[]> reader = mock(Reader.class);
    @SuppressWarnings("unchecked")
    Message<byte[]> message = mock(Message.class);

    CompletableFuture<java.util.List<String>> failedPartitions = new CompletableFuture<>();
    failedPartitions.completeExceptionally(new PulsarClientException("Connection already closed"));
    when(staleClient.getPartitionsForTopic("persistent://acme/orders/payment-events")).thenReturn(failedPartitions);

    when(freshClient.getPartitionsForTopic("persistent://acme/orders/payment-events"))
        .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));
    when(freshClient.newReader()).thenReturn(readerBuilder);
    when(readerBuilder.topic("persistent://acme/orders/payment-events")).thenReturn(readerBuilder);
    when(readerBuilder.startMessageFromRollbackDuration(3650, java.util.concurrent.TimeUnit.DAYS)).thenReturn(readerBuilder);
    when(readerBuilder.create()).thenReturn(reader);
    when(reader.readNext(250, java.util.concurrent.TimeUnit.MILLISECONDS)).thenReturn(message, null);

    when(message.getMessageId()).thenReturn(mock(org.apache.pulsar.client.api.MessageId.class));
    when(message.getMessageId().toString()).thenReturn("ledger:91:2050");
    when(message.hasKey()).thenReturn(false);
    when(message.getPublishTime()).thenReturn(1_742_240_020_000L);
    when(message.getEventTime()).thenReturn(0L);
    when(message.getProducerName()).thenReturn("peek-producer");
    when(message.getData()).thenReturn("{\"state\":\"WAITING\"}".getBytes());
    when(message.getSchemaVersion()).thenReturn(null);

    RestPulsarAdminGateway.PulsarClientFactory factory = mock(RestPulsarAdminGateway.PulsarClientFactory.class);
    when(factory.create(environment())).thenReturn(staleClient, freshClient);

    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(
        RestClient.builder().build(),
        new ObjectMapper(),
        factory);

    PeekMessagesResponse response = gateway.peekMessages(environment(), "persistent://acme/orders/payment-events", 1);

    assertThat(response.returnedCount()).isEqualTo(1);
    verify(factory, times(2)).create(environment());
    verify(staleClient).close();
    verify(reader).close();
  }

  @Test
  void shouldPublishMessageThroughPulsarClient() throws Exception {
    PulsarClient client = mock(PulsarClient.class);
    @SuppressWarnings("unchecked")
    ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
    @SuppressWarnings("unchecked")
    Producer<byte[]> producer = mock(Producer.class);
    @SuppressWarnings("unchecked")
    TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);
    MessageId messageId = mock(MessageId.class);

    when(client.newProducer()).thenReturn(producerBuilder);
    when(producerBuilder.topic("persistent://acme/orders/payment-events")).thenReturn(producerBuilder);
    when(producerBuilder.create()).thenReturn(producer);
    when(producer.newMessage()).thenReturn(messageBuilder);
    when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
    when(messageBuilder.key("payment-1")).thenReturn(messageBuilder);
    when(messageBuilder.properties(Map.of("traceId", "abc-123"))).thenReturn(messageBuilder);
    when(messageBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(messageId));
    when(messageId.toString()).thenReturn("ledger:91:2048");

    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(
        RestClient.builder().build(),
        new ObjectMapper(),
        environment -> client);

    var response = gateway.publishMessage(environment(), new PublishMessageRequest(
        "persistent://acme/orders/payment-events",
        "payment-1",
        Map.of("traceId", "abc-123"),
        "RAW",
        "{\"event\":\"test\"}",
        "Operator publish test"));

    assertThat(response.messageId()).isEqualTo("ledger:91:2048");
    assertThat(response.message()).contains("Published a test message");
    verify(producer).close();
  }

  @Test
  void shouldConsumeMessagesThroughPulsarClient() throws Exception {
    PulsarClient client = mock(PulsarClient.class);
    @SuppressWarnings("unchecked")
    ConsumerBuilder<byte[]> consumerBuilder = mock(ConsumerBuilder.class);
    @SuppressWarnings("unchecked")
    Consumer<byte[]> consumer = mock(Consumer.class);
    @SuppressWarnings("unchecked")
    Message<byte[]> message = mock(Message.class);
    MessageId messageId = mock(MessageId.class);

    when(client.newConsumer()).thenReturn(consumerBuilder);
    when(consumerBuilder.topic("persistent://acme/orders/payment-events")).thenReturn(consumerBuilder);
    when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
    when(consumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(consumerBuilder);
    when(consumerBuilder.subscriptionInitialPosition(any(SubscriptionInitialPosition.class))).thenReturn(consumerBuilder);
    when(consumerBuilder.subscribe()).thenReturn(consumer);

    @SuppressWarnings("unchecked")
    Message<byte[]> noMessage = (Message<byte[]>) null;
    when(consumer.receive(anyInt(), any(TimeUnit.class))).thenReturn(message, noMessage);
    when(message.getMessageId()).thenReturn(messageId);
    when(messageId.toString()).thenReturn("ledger:91:2048");
    when(message.hasKey()).thenReturn(true);
    when(message.getKey()).thenReturn("payment-1");
    when(message.getPublishTime()).thenReturn(1_742_240_000_000L);
    when(message.getEventTime()).thenReturn(0L);
    when(message.getProperties()).thenReturn(Map.of("traceId", "abc-123"));
    when(message.getProducerName()).thenReturn("payments-producer-1");
    when(message.getData()).thenReturn("{\"event\":\"test\"}".getBytes());

    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(
        RestClient.builder().build(),
        new ObjectMapper(),
        environment -> client);

    var response = gateway.consumeMessages(environment(), new ConsumeMessagesRequest(
        "persistent://acme/orders/payment-events",
        "payment-sub",
        false,
        2,
        1,
        "Operator consume test"));

    assertThat(response.receivedCount()).isEqualTo(1);
    assertThat(response.messages()).hasSize(1);
    assertThat(response.messages().get(0).key()).isEqualTo("payment-1");
    verify(consumer).acknowledge(message);
    verify(consumer).close();
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
  void shouldCreateNonPartitionedTopicViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events-retry"))
        .andRespond(withSuccess());

    gateway.createTopic(environment(), new CreateTopicRequest(
        "persistent",
        "acme",
        "orders",
        "payment-events-retry",
        0,
        "Retry topic"));

    server.verify();
  }

  @Test
  void shouldCreateTenantViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/tenants/finance"))
        .andRespond(withSuccess());

    gateway.createTenant(environment(), new CreateTenantRequest(
        "finance",
        java.util.List.of("platform-admin"),
        java.util.List.of("prod-east")));

    server.verify();
  }

  @Test
  void shouldCreateNamespaceViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/namespaces/acme/sandbox"))
        .andRespond(withSuccess());

    gateway.createNamespace(environment(), new CreateNamespaceRequest("acme", "sandbox"));

    server.verify();
  }

  @Test
  void shouldCreateSubscriptionViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review"))
        .andRespond(withSuccess());
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review/resetcursor/0"))
        .andRespond(withSuccess());

    gateway.createSubscription(environment(), new CreateSubscriptionRequest(
        "persistent://acme/orders/payment-events",
        "payment-review",
        "EARLIEST",
        "Create review subscription"));

    server.verify();
  }

  @Test
  void shouldDeleteSubscriptionViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review"))
        .andRespond(withSuccess());

    gateway.deleteSubscription(environment(), "persistent://acme/orders/payment-events", "payment-review");

    server.verify();
  }

  @Test
  void shouldDiscoverLiveClustersWhenCreatingTenantWithoutAllowedClusters() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/clusters"))
        .andRespond(withSuccess("""
            ["standalone"]
            """, MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/tenants/smtp"))
        .andExpect(content().json("""
            {
              "adminRoles": [],
              "allowedClusters": ["standalone"]
            }
            """))
        .andRespond(withSuccess());

    gateway.createTenant(environment(), new CreateTenantRequest("smtp", List.of(), List.of()));

    server.verify();
  }

  @Test
  void shouldFallbackToStandaloneSkipAllPathWhenLatestCursorEndpointDiffers() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review/skip_all/skipAllMessages"))
        .andRespond(withStatus(HttpStatus.NOT_FOUND));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review/skip_all"))
        .andRespond(withSuccess());

    var response = gateway.resetCursor(environment(), new com.pulsaradmin.shared.model.ResetCursorRequest(
        "persistent://acme/orders/payment-events",
        "payment-review",
        "LATEST",
        null,
        "Move to the latest position"));

    assertThat(response.target()).isEqualTo("LATEST");
    assertThat(response.message()).contains("latest position");
    server.verify();
  }

  @Test
  void shouldFallbackToStandaloneSkipPathWhenSkipMessagesEndpointDiffers() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review/skip/1/skipMessages"))
        .andRespond(withStatus(HttpStatus.NOT_FOUND));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/subscription/payment-review/skip/1"))
        .andRespond(withSuccess());

    var response = gateway.skipMessages(environment(), new com.pulsaradmin.shared.model.SkipMessagesRequest(
        "persistent://acme/orders/payment-events",
        "payment-review",
        1,
        "Skip one poison message"));

    assertThat(response.skippedMessages()).isEqualTo(1);
    server.verify();
  }

  @Test
  void shouldUnloadTopicViaAdminRest() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    RestPulsarAdminGateway gateway = new RestPulsarAdminGateway(builder.build(), new ObjectMapper());

    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/unload"))
        .andRespond(withSuccess());
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/partitions"))
        .andRespond(withSuccess("""
            {"partitions": 0}
            """, MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/persistent/acme/orders/payment-events/stats"))
        .andRespond(withSuccess("""
            {
              "msgBacklog": 18720,
              "publishRateIn": 190.5,
              "msgRateOut": 48.3,
              "storageSize": 5880120,
              "publishers": [
                {"producerName": "payments-producer-1"}
              ],
              "subscriptions": {
                "payment-settlement": {},
                "payment-alerts": {}
              }
            }
            """, MediaType.APPLICATION_JSON));
    server.expect(requestTo("https://pulsar-admin.prod.example.com/admin/v2/schemas/acme/orders/payment-events/schema"))
        .andRespond(withSuccess("""
            {
              "type": "JSON",
              "version": "9",
              "data": {}
            }
            """, MediaType.APPLICATION_JSON));

    var response = gateway.unloadTopic(environment(), new UnloadTopicRequest(
        "persistent://acme/orders/payment-events",
        "Refresh broker ownership"));

    assertThat(response.topicName()).isEqualTo("persistent://acme/orders/payment-events");
    assertThat(response.message()).contains("Unloaded topic");
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
        null,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        null,
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
        null,
        true,
        "SYNCED",
        "Metadata synced successfully.",
        null,
        Instant.parse("2026-03-17T18:00:00Z"),
        "SUCCESS",
        "Connection verified.",
        Instant.parse("2026-03-17T17:59:00Z"),
        false);
  }
}
