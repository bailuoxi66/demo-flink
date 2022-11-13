package com.example.orderpay_detect.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {
    private Long orderId;
    private String resultState;
}