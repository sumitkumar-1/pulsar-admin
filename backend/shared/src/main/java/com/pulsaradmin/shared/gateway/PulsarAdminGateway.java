package com.pulsaradmin.shared.gateway;

import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;

public interface PulsarAdminGateway {
  EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment);

  EnvironmentSnapshot syncMetadata(EnvironmentDetails environment);

  void createTopic(EnvironmentDetails environment, CreateTopicRequest request);

  void createSubscription(EnvironmentDetails environment, CreateSubscriptionRequest request);

  void deleteSubscription(EnvironmentDetails environment, String topicName, String subscriptionName);

  PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit);

  ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request);

  SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request);
}
