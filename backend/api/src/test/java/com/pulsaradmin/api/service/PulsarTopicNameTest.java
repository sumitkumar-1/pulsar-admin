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
  }

  @Test
  void shouldRejectMalformedTopicName() {
    assertThatThrownBy(() -> PulsarTopicName.parse("acme/orders/payment-events"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("domain");
  }
}
