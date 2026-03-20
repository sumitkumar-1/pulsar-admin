package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.Map;

public record ConsumedMessage(
    String messageId,
    String key,
    Instant publishTime,
    Instant eventTime,
    Map<String, String> properties,
    String producerName,
    String payload) {
}
