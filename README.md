# internet_expand
对用户上网记录的ETL处理

##DO
hadoop jar internet_expand-1.0-SNAPSHOT.jar 2017011107

## INPUT
20170226045845,internet,58696ca141c7,cc2d83e1f0c4,http%3A//apilocate.amap.com/mobile/binary%3Foutput%3Djson,*
* 20170226045845: 日期字符串
* internet: 数据类型
* 58696ca141c7: 盒子mac
* cc2d83e1f0c4: 用户mac
* http%3A//apilocate.amap.com/mobile/binary%3Foutput%3Djson ： encode_url
* *: 用户手机号，没有用*表示

 ## OUTPUT
20170226045845,internet,58696ca141c7,cc2d83e1f0c4,http%3A//apilocate.amap.com/mobile/binary%3Foutput%3Djson,*,http,apilocate.amap.com,/mobile/binary,output=json,1001001,664路
* 20170226045845：日期字符串
* internet： 数据类型
* 58696ca141c7： 盒子mac
* cc2d83e1f0c4： 用户mac
* http%3A//apilocate.amap.com/mobile/binary%3Foutput%3Djson： encode_url
* *: 用户手机号，没有用*表示
* http: 协议
* apilocate.amap.com： host
* /mobile/binary： path
* output=json：query
* 1001001： 城市编码
* 664路： 路线名称



