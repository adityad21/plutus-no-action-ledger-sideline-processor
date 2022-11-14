package com.phonepe.accounting.ledger.sideline.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SidelineEventPayload {
  private String transactionId;
  private String eventType;
  private String actionType;
  private String kafkaTopic;
}
