package com.example.orderpay_detect.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long timestamp;
}
