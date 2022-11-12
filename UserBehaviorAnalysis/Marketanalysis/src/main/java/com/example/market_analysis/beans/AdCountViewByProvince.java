package com.example.market_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
