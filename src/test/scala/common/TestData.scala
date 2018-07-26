package common

import org.apache.spark.sql.DataFrame

trait TestData extends SparkSessionWrapperTest{
  import spark.implicits._
    val hourlyData = Seq(
      ("10/03/2004","18.00.00","2,6","1360","150","11,9","1046","166","1056","113","1692","1268","13,6","48,9","0,7578",""),
      ("10/03/2004","19.00.00","2","1292","112","9,4","955","103","1174","92","1559","972","13,3","47,7","0,7255",""),
      ("10/03/2004","20.00.00","2,2","1402","88","9,0","939","131","1140","114","1555","1074","11,9","54,0","0,7502",""),
      ("10/03/2004","21.00.00","2,2","1376","80","9,2","948","172","1092","122","1584","1203","11,0","60,0","0,7867",""),
      ("10/03/2004","22.00.00","1,6","1272","51","6,5","836","131","1205","116","1490","1110","11,2","59,6","0,7888",""),
      ("10/03/2004","23.00.00","1,2","1197","38","4,7","750","89","1337","96","1393","949","11,2","59,2","0,7848",""),
      ("11/03/2004","00.00.00","1,2","1185","31","3,6","690","62","1462","77","1333","733","11,3","56,8","0,7603",""),
      ("11/03/2004","01.00.00","1","1136","31","3,3","672","62","1453","76","1333","730","10,7","60,0","0,7702",""),
      ("11/03/2004","02.00.00","0,9","1094","24","2,3","609","45","1579","60","1276","620","10,7","59,7","0,7648",""),
      ("11/03/2004","03.00.00","0,6","1010","19","1,7","561","-200","1705","-200","1235","501","10,3","60,2","0,7517",""),
      ("11/03/2004","04.00.00","-200","1011","14","1,3","527","21","1818","34","1197","445","101","60,5","0,7465",""),
      ("11/03/2004","05.00.00","0,7","1066","8","11","512","16","1918","28","1182","422","11,0","56,2","0,7366",""),
      ("11/03/2004","06.00.00","0,7","1052","16","1,6","553","34","1738","48","1221","472","10,5","581","0,7353",""),
      ("11/03/2004","07.00.00","11","1144","29","3,2","667","98","1490","82","1339","730","10,2","59,6","0,7417",""),
      ("11/03/2004","08.00.00","2","1333","64","8,0","900","174","1136","112","1517","1102","10,8","57,4","0,7408",""),
      ("11/03/2004","09.00.00","2,2","1351","87","9,5","960","129","1079","101","1583","1028","10,5","60,6","0,7691",""),
      ("11/03/2004","10.00.00","1,7","1233","77","6,3","827","112","1218","98","1446","860","10,8","58,4","0,7552",""),
      ("11/03/2004","11.00.00","1,5","1179","43","5,0","762","95","1328","92","1362","671","10,5","57,9","0,7352",""),
      ("11/03/2004","12.00.00","1,6","1236","61","5,2","774","104","1301","95","1401","664","9,5","66,8","0,7951",""),
      ("11/03/2004","13.00.00","1,9","1286","63","7,3","869","146","1162","112","1537","799","8,3","76,4","0,8393",""),
      ("11/03/2004","14.00.00","2,9","1371","164","11,5","1034","207","983","128","1730","1037","8,0","811","0,8736",""),
      ("11/03/2004","15.00.00","2,2","1310","79","8,8","933","184","1082","126","1647","946","8,3","79,8","0,8778",""),
      ("11/03/2004","16.00.00","2,2","1292","95","8,3","912","193","1103","131","1591","957","9,7","71,2","0,8569",""),
      ("11/03/2004","17.00.00","2,9","1383","150","11,2","1020","243","1008","135","1719","1104","9,8","67,6","0,8185",""),
      ("11/03/2004","18.00.00","4,8","1581","307","20,8","1319","281","799","151","2083","1409","10,3","64,2","0,8065",""),
      ("11/03/2004","19.00.00","6,9","1776","461","27,4","1488","383","702","172","2333","1704","9,7","69,3","0,8319",""),
      ("11/03/2004","20.00.00","61","1640","401","24,0","1404","351","743","165","2191","1654","9,6","67,8","0,8133",""),
      ("11/03/2004","21.00.00","3,9","1313","197","12,8","1076","240","957","136","1707","1285","91","64,0","0,7419",""),
      ("11/03/2004","22.00.00","1,5","965","61","4,7","749","94","1325","85","1333","821","8,2","63,4","0,6905",""),
      ("11/03/2004","23.00.00","1","913","26","2,6","629","47","1565","53","1252","552","8,2","60,8","0,6657","")
    )

  val dailyData = List()
  val hourlyDataDf =  hourlyData.toDF("Date","Time","CO(GT)","PT08.S1(CO)","NMHC(GT)","C6H6(GT)","PT08.S2(NMHC)","NOx(GT)","PT08.S3(NOx)","NO2(GT)","PT08.S4(NO2)","PT08.S5(O3)","T","RH","AH")

//  def createDF(data : Seq[Any], header: Array[String]): DataFrame = {
//    data.toDF("Date","Time","CO(GT)","PT08.S1(CO)","NMHC(GT)","C6H6(GT)","PT08.S2(NMHC)","NOx(GT)","PT08.S3(NOx)","NO2(GT)","PT08.S4(NO2)","PT08.S5(O3)","T","RH","AH")
//  }
}