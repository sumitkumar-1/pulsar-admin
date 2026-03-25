package com.pulsaradmin.api.service;

import java.util.List;
import java.util.Map;

public record ReplayCopyCriteriaInput(
    List<String> headers,
    List<Map<String, String>> rows,
    List<String> errors) {
  public static ReplayCopyCriteriaInput empty() {
    return new ReplayCopyCriteriaInput(List.of(), List.of(), List.of());
  }
}
