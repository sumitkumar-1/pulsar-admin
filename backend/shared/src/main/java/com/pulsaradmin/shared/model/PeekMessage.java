package com.pulsaradmin.shared.model;

public record PeekMessage(
    String messageId,
    String key,
    String publishTime,
    String eventTime,
    String producerName,
    String summary,
    String payload,
    String schemaVersion) {
}
