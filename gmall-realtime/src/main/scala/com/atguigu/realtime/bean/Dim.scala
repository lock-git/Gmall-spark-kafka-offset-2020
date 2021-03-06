package com.atguigu.realtime.bean

import java.text.SimpleDateFormat

case class UserInfo(id: String,
                    user_level: String,
                    birthday: String,
                    gender: String, // F  M
                    var age_group: String = null, //年龄段
                    var gender_name: String = null) { //性别  男  女
    // 计算年龄段
    val age = (System.currentTimeMillis() - new SimpleDateFormat("yyyy-MM-dd").parse(birthday).getTime) / 1000 / 60 / 60 / 24 / 365
    age_group = if (age <= 20) "20岁及以下" else if (age <= 30) "21岁到 30 岁" else "30岁及以上"
    // 计算gender_name
    gender_name = if (gender == "F") "女" else "男"
}


case class ProvinceInfo(id: String,
                        name: String,
                        area_code: String,
                        iso_code: String)


case class BaseTrademark(tm_id:String , tm_name:String)

case class BaseCategory3(id: String,
                         name: String,
                         category2_id: String)
case class SpuInfo(id: String, spu_name: String)

case class SkuInfo(id: String,
                   spu_id: String,
                   price: String,
                   sku_name: String,
                   tm_id: String,
                   category3_id: String,
                   create_time: String,

                   var category3_name: String = null,
                   var spu_name: String = null,
                   var tm_name: String = null)


