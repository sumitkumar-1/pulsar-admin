package com.pulsaradmin.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.pulsaradmin")
public class PulsarAdminApiApplication {
  public static void main(String[] args) {
    SpringApplication.run(PulsarAdminApiApplication.class, args);
  }
}
