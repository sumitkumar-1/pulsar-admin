package com.pulsaradmin.api.service;

record PulsarTopicName(String domain, String tenant, String namespace, String topic) {
  private static final String PARTITION_SUFFIX = "-partition-";

  static PulsarTopicName parse(String fullName) {
    if (fullName == null || fullName.isBlank()) {
      throw new IllegalArgumentException("Topic name is required.");
    }

    String[] domainSplit = fullName.split("://", 2);
    if (domainSplit.length != 2) {
      throw new IllegalArgumentException("Topic name must include a domain, for example persistent://tenant/ns/topic.");
    }

    String[] pathSegments = domainSplit[1].split("/", 3);
    if (pathSegments.length != 3) {
      throw new IllegalArgumentException("Topic name must include tenant, namespace, and topic segments.");
    }

    return new PulsarTopicName(domainSplit[0], pathSegments[0], pathSegments[1], pathSegments[2]);
  }

  String adminTopicPath() {
    return tenant + "/" + namespace + "/" + topic;
  }

  String namespacePath() {
    return tenant + "/" + namespace;
  }

  String fullName() {
    return domain + "://" + tenant + "/" + namespace + "/" + topic;
  }

  String canonicalTopic() {
    int partitionSuffix = topic.lastIndexOf(PARTITION_SUFFIX);
    if (partitionSuffix < 0) {
      return topic;
    }

    String suffixValue = topic.substring(partitionSuffix + PARTITION_SUFFIX.length());
    if (suffixValue.chars().allMatch(Character::isDigit)) {
      return topic.substring(0, partitionSuffix);
    }

    return topic;
  }

  String canonicalFullName() {
    return domain + "://" + tenant + "/" + namespace + "/" + canonicalTopic();
  }

  Integer partitionIndex() {
    int partitionSuffix = topic.lastIndexOf(PARTITION_SUFFIX);
    if (partitionSuffix < 0) {
      return null;
    }

    String suffixValue = topic.substring(partitionSuffix + PARTITION_SUFFIX.length());
    if (!suffixValue.chars().allMatch(Character::isDigit)) {
      return null;
    }

    return Integer.parseInt(suffixValue);
  }
}
