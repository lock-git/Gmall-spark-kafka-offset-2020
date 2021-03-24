package com.atguigu.realtime.bean

case class OrderDetail(id: Long,
                       order_id: Long,
                       sku_id: Long,
                       order_price: Double,
                       sku_num: Long,
                       sku_name: String,
                       create_time: String,

                       var spu_id: Long = 0L, //作为维度数据 要关联进来
                       var tm_id: Long = 0L,
                       var category3_id: Long = 0L,
                       var spu_name: String = null,
                       var tm_name: String = null,
                       var category3_name: String = null) {
   
   
    def mergeSkuInfo(skuInfo: SkuInfo) = {
        this.spu_id = skuInfo.spu_id.toLong
        this.tm_id = skuInfo.tm_id.toLong
        this.category3_id = skuInfo.category3_id.toLong
        this.spu_name = skuInfo.spu_name
        this.tm_name = skuInfo.tm_name
        this.category3_name = skuInfo.category3_name
        this
    }
}

