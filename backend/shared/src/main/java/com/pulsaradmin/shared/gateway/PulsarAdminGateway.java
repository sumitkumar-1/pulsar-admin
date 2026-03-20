package com.pulsaradmin.shared.gateway;

import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
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

  ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request);

  SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request);

  UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request);
}
