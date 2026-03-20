package com.pulsaradmin.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PulsarTopicNameTest {
  @Test
  void shouldParsePersistentTopicName() {
    PulsarTopicName parsed = PulsarTopicName.parse("persistent://acme/orders/payment-events");

    assertThat(parsed.domain()).isEqualTo("persistent");
    assertThat(parsed.tenant()).isEqualTo("acme");
    assertThat(parsed.namespace()).isEqualTo("orders");
    assertThat(parsed.topic()).isEqualTo("payment-events");
    assertThat(parsed.adminTopicPath()).isEqualTo("acme/orders/payment-events");
    assertThat(parsed.canonicalFullName()).isEqualTo("persistent://acme/orders/payment-events");
  }

  @Test
  void shouldCanonicalizePartitionTopicName() {
    PulsarTopicName parsed = PulsarTopicName.parse("persistent://acme/orders/payment-events-partition-7");

    assertThat(parsed.topic()).isEqualTo("payment-events-partition-7");
    assertThat(parsed.canonicalTopic()).isEqualTo("payment-events");
    assertThat(parsed.canonicalFullName()).isEqualTo("persistent://acme/orders/payment-events");
    assertThat(parsed.partitionIndex()).isEqualTo(7);
  }

  @Test
  void shouldRejectMalformedTopicName() {
    assertThatThrownBy(() -> PulsarTopicName.parse("acme/orders/payment-events"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("domain");
  }
}
