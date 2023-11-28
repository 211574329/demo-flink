package com.echo.ranking.poly;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * flink统计对象
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AreaProductRanking implements Serializable {

    private static final long serialVersionUID = 6836530909471066135L;

    /**
     * 产品sku-id
     */
    private Long skuId;

    /**
     * sku名称
     */
    private String skuName;

    /**
     * 商品购买数量
     */
    private Integer count;

    /**
     * 用户所在地区编号
     */
    private Integer areaId;

    /**
     * 用户所在地区名称
     */
    private String areaName;

}
