package com.pulsaradmin.shared.gateway;

import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import java.util.List;
import java.util.Optional;

public interface PulsarAdminGateway {
  List<EnvironmentSummary> getEnvironments();

  Optional<EnvironmentHealth> getEnvironmentHealth(String environmentId);

  PagedResult<TopicListItem> getTopics(
      String environmentId,
      String tenant,
      String namespace,
      String search,
      int page,
      int pageSize);

  Optional<TopicDetails> getTopicDetails(String environmentId, String fullTopicName);
}
