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
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;

public interface PulsarAdminGateway {
  EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment);

  EnvironmentSnapshot syncMetadata(EnvironmentDetails environment);

  void createTopic(EnvironmentDetails environment, CreateTopicRequest request);

  void createTenant(EnvironmentDetails environment, CreateTenantRequest request);

  void createNamespace(EnvironmentDetails environment, CreateNamespaceRequest request);

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

  ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request);

  SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request);

  UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request);
}
