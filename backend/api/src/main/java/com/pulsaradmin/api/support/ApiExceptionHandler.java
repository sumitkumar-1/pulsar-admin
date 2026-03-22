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

  @ExceptionHandler(Throwable.class)
  public ResponseEntity<ApiErrorResponse> handleUnexpected(
      Throwable exception,
      HttpServletRequest request) {
    String message = exception.getMessage() == null || exception.getMessage().isBlank()
        ? "Unexpected server error."
        : exception.getMessage();
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new ApiErrorResponse(
            Instant.now(),
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            message,
            request.getRequestURI()));
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
