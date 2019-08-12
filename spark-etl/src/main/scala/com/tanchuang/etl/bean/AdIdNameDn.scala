package com.tanchuang.etl.bean

import java.sql.Timestamp

import org.apache.spark.sql.types.Decimal


case class AdIdNameDn(adid: Int, adname: String, dn: String) 

case class Basewewebsite(createtime: String, creator: String, delete: Int, 
                         dn: String, siteid: Int, sitename: String, siteurl: String) 

case class Dwd_member(
                       uid: Int,
                       ad_id: Int,
                       birthday: String,
                       email: String,
                       var fullname: String,
                       iconurl: String,
                       lastlogin: String,
                       mailaddr: String,
                       memberlevel: String,
                       var password: String,
                       paymoney: String,
                       var phone: String,
                       qq: String,
                       register: String,
                       regupdatetime: String,
                       unitname: String,
                       userip: String,
                       zipcode: String,
                       var dt: String,
                       dn: String
                     )   


//{"appkey":"-",
// "appregurl":"http:www.webA.com/sale/register/index.html",
// "bdp_uuid":"-",
// "createtime":"2017-03-30",
// "dn":"webA",
// "domain":"-",
// "dt":"20190722","
// isranreg":"-",
// "regsource":"0",
// "uid":"0",
// "websiteid":"4"}
case class MemberRegType(
                          uid: Int,
                          appkey: String,
                          appregurl: String,
                          bdp_uuid: String,
                          createtime: String,
                          domain: String,
                          isranreg: String,
                          regsource: String,
                          websiteid: Int,
                          dt: String,
                          dn: String
                        )   


//{"dn":"webA","dt":"20190722","paymoney":"340.87","siteid":"2","uid":"51178","vip_id":"2"}
case class PayMoney(
                     uid: Int,
                     paymoney: String,
                     siteid: Int,
                     Vip_id: Int,
                     dt: String,
                     dn: String) 


case class VipLevel(vip_id: Int,
                    vip_level: String,
                    var start_time: String,
                    var end_time: String,
                    var last_modify_time: String,
                    max_free: String,
                    min_free: String,
                    next_level: String,
                    operator: String,
                    dn: String)  


case class Dws_Member(uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: String,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: Timestamp,
                      domain: String,
                      isranreg: String,
                      regsource: String,
                      adname: String,
                      siteid: Int,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: Int,
                      vip_level: String,
                      vip_start_time: Timestamp,
                      vip_end_time: Timestamp,
                      vip_last_modify_time: Timestamp,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String)  


/* select t1.uid,sum(cast(t1.paymoney as decimal(10,4))) paymoney,max(t2.vip_level) as vip_level,
 from_unixtime(unix_timestamp("$dayTime","yyyyMMdd"),"yyyy-MM-dd") as start_time,"9999-12-31" as end_time, t1.dn
 *
 */
case class MemberZipper(uid: Int, var paymoney: String, vip_level: String,
                        start_time: Timestamp, var end_time: Timestamp, dn: String)   


case class MemberZipperResult(membersList: List[MemberZipper])   


case class Ads_Register_Appregurlnum(appregurl: String,
                                     num: BigInt,
                                     dt: String,
                                     dn: String)  


case class Ads_Register_Sitenamenum(sitename: String,
                                    num: BigInt,
                                    dt: String,
                                    dn: String)  


case class Ads_Register_Regsourcename(regsourcename: String,
                                      num: BigInt,
                                      dt: String,
                                      dn: String)  


case class Ads_Register_Adnamenum(adname: String,
                                  num: BigInt,
                                  dt: String,
                                  dn: String)  



case class Ads_Register_Memberlevelnum(memberlevel: String,
                                       num: BigInt,
                                       dt: String,
                                       dn: String)  


case class Ads_Register_Top3MemberPay(uid: Int,
                                      memberlevel: String,
                                      register: String,
                                      appregurl: String,
                                      regsourcename: String,
                                      adname: String,
                                      sitename: String,
                                      vip_level: String,
                                      paymoney: Decimal,
                                      rownum: Int,
                                      dt: String,
                                      dn: String)  
