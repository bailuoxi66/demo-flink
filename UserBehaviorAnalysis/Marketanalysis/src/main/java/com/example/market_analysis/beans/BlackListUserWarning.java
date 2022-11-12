package com.example.market_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;
}
