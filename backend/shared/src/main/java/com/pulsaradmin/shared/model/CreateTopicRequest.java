package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateTopicRequest(
    @NotBlank(message = "Topic domain is required.")
    String domain,
    @NotBlank(message = "Tenant is required.")
    String tenant,
    @NotBlank(message = "Namespace is required.")
    String namespace,
    @NotBlank(message = "Topic name is required.")
    String topic,
    @Min(value = 0, message = "Partition count must be zero or greater.")
    @Max(value = 128, message = "Partition count must be 128 or less in this prototype.")
    int partitions,
    String notes) {

  public String fullTopicName() {
    return domain + "://" + tenant + "/" + namespace + "/" + topic;
  }
}
