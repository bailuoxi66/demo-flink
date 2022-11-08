package com.example.networkflow_analysis.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;
}
