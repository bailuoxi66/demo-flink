package com.example.hotitems_analysis.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
