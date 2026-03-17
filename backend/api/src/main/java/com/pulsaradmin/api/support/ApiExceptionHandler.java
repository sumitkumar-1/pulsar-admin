package com.pulsaradmin.api.support;

import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<ApiErrorResponse> handleNotFound(
      NotFoundException exception,
      HttpServletRequest request) {
    return build(HttpStatus.NOT_FOUND, exception, request);
  }

  @ExceptionHandler(BadRequestException.class)
  public ResponseEntity<ApiErrorResponse> handleBadRequest(
      BadRequestException exception,
      HttpServletRequest request) {
    return build(HttpStatus.BAD_REQUEST, exception, request);
  }

  private ResponseEntity<ApiErrorResponse> build(
      HttpStatus status,
      RuntimeException exception,
      HttpServletRequest request) {
    return ResponseEntity.status(status)
        .body(new ApiErrorResponse(
            Instant.now(),
            status.value(),
            status.getReasonPhrase(),
            exception.getMessage(),
            request.getRequestURI()));
  }
}
