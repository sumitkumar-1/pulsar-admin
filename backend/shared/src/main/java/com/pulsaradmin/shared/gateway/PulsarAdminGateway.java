package com.pulsaradmin.shared.gateway;

import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PlatformArtifactDetails;
import com.pulsaradmin.shared.model.PlatformArtifactMutationRequest;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SchemaDetails;
import com.pulsaradmin.shared.model.SchemaUpdateRequest;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.util.List;

public interface PulsarAdminGateway {
  EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment);

  EnvironmentSnapshot syncMetadata(EnvironmentDetails environment);

  void createTopic(EnvironmentDetails environment, CreateTopicRequest request);

  void updateTopicPartitions(EnvironmentDetails environment, String topicName, int partitions);

  void createTenant(EnvironmentDetails environment, CreateTenantRequest request);

  void createNamespace(EnvironmentDetails environment, CreateNamespaceRequest request);

  TenantDetails getTenantDetails(EnvironmentDetails environment, String tenant);

  TenantDetails updateTenant(EnvironmentDetails environment, TenantUpdateRequest request);

  void deleteTenant(EnvironmentDetails environment, String tenant);

  void createSubscription(EnvironmentDetails environment, CreateSubscriptionRequest request);

  void deleteSubscription(EnvironmentDetails environment, String topicName, String subscriptionName);

  PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit);

  TerminateTopicResponse terminateTopic(EnvironmentDetails environment, TerminateTopicRequest request);

  TopicPolicies getTopicPolicies(EnvironmentDetails environment, String topicName);

  TopicPolicies updateTopicPolicies(EnvironmentDetails environment, String topicName, TopicPolicies policies);

  NamespaceDetails getNamespaceDetails(EnvironmentDetails environment, String tenant, String namespace);

  NamespacePolicies updateNamespacePolicies(
      EnvironmentDetails environment,
      String tenant,
      String namespace,
      NamespacePolicies policies);

  PublishMessageResponse publishMessage(EnvironmentDetails environment, PublishMessageRequest request);

  ConsumeMessagesResponse consumeMessages(EnvironmentDetails environment, ConsumeMessagesRequest request);

  void deleteTopic(EnvironmentDetails environment, String topicName);

  void deleteNamespace(EnvironmentDetails environment, String tenant, String namespace);

  PlatformSummary getPlatformSummary(EnvironmentDetails environment, List<String> namespaces);

  PlatformArtifactDetails getPlatformArtifactDetails(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name);

  PlatformArtifactDetails upsertPlatformArtifact(
      EnvironmentDetails environment,
      PlatformArtifactMutationRequest request);

  void deletePlatformArtifact(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name);

  SchemaDetails getSchemaDetails(EnvironmentDetails environment, String topicName);

  SchemaDetails upsertSchema(EnvironmentDetails environment, SchemaUpdateRequest request);

  void deleteSchema(EnvironmentDetails environment, String topicName);

  ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request);

  SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request);

  UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request);
}
