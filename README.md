# MusicProject
本项目用于从不同数据源中提取相关数据到Hive中，按照数仓分层理论将每个业务处理过程分为三层，分别为ODS层、EDS层以及DM层，项目架构如下图所示：
![image](https://user-images.githubusercontent.com/74764934/223391578-41f7ec9b-ac62-4da0-8d67-81cdf536ee52.png)
## 1.base
定义了MultipleTextOutputFormat的子类PairRDDMultipleTextOutputFormat，表示有键值对的RDD，用于将清洗完成的用户行为日志数据存放在HDFS上，便于后续将其存储在Hive的ODS中
## 2.common
分别定义了ConfigUtils、DateUtils以及StringUtils对象，用于获取集群环境相关配置、日期格式转换与字符串校验
## 3.ODS层
ProduceClient用于提取并清洗运维人员每日上传的用户行为日志并将其存储在Hive数仓的ODS层表中
## 4.EDS层
针对音乐内容、点播机器以及用户活跃三个主题，构建了相应对象，用于满足数据的提取、清洗、表连接与表创建等业务需求，进而生成DWD与DWS层数据表
## 5.DM层
针对音乐内容、点播机器以及用户活跃三个主题，用于从DW层提取相关数据到DM层，并生成相关的MySQL表，为下一步SuperSet数据可视化做准备
## 业务模型分区设计
![image](https://user-images.githubusercontent.com/74764934/223397943-c43bd250-0106-4239-9116-417fb01ad7bd.png)

