package com.example.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author laneliang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorAddrReading {

    /**
     * 传感器id
     */
    private String id;

    /**
     * 区域
     */
    private String area;

    /**
     * 具体时间
     */
    private Long timestamp;

    /**
     * 具体温度示数
     */
    private Double temperature;
}