package com.pulsaradmin.shared.model;

import java.util.List;

public record PagedResult<T>(
    List<T> items,
    int page,
    int pageSize,
    long total) {
}
