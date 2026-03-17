package com.pulsaradmin.shared.model;

public record TopicStatsSummary(
    long backlog,
    int producers,
    int subscriptions,
    int consumers,
    double publishRateIn,
    double dispatchRateOut,
    double throughputIn,
    double throughputOut,
    long storageSize) {
}
