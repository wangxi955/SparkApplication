# SparkApplication

## 本仓库为 Spark 课程设计存档，主要分为四个任务

### 搭建 Hadoop 和 Spark 集群

#### 搭建 Hadoop

准备工作：

1. 选定一台机器作为 Master
2. 在 Master 节点上配置 hadoop 用户、安装 SSH server、安装 Java 环境
3. 在 Master 节点上安装 Hadoop，并完成配置
4. 在其他 Slave 节点上配置 hadoop 用户、安装 SSH server、安装 Java 环境（可采用脚本分发）
5. 将 Master 节点上的 `/usr/local/hadoop` 目录复制到其他 Slave 节点上
6. 在 Master 节点上开启 Hadoop

网络配置：

1. 搭建 5 台机器组成集群模式，s198hadoop 作为主节点，其余作为从节点。
   - 10.244.3.85 s198hadoop
   - 10.244.3.88 s193hadoop
   - 10.244.3.86 s194hadoop
   - 10.244.3.69 s195hadoop
   - 10.244.3.2  s197hadoop


2. 分别修改 5 台虚拟机的 `/etc/hosts` 文件，编写集群节点主机名与 IP 地址的映射关系

3. 集群/分布式模式配置：

   - 集群/分布式模式需要修改 `/usr/local/hadoop/etc/hadoop` 中的 5 个配置文件：`workers`、`core-site.xml`、`hdfs-site.xml`、`mapred-site.xml`、`yarn-site.xml` 文件

   - 在主节点上将公钥传输到从节点，在从节点将 SSH 公钥加入授权

4. 可从 [http://s198hadoop:8088/cluster/](http://s198hadoop:8088/cluster/) 查看 Hadoop 集群

#### 搭建 Spark

1. 集群分布式配置：

   修改 `/usr/local/spark` 文件夹下的 `slaves`、`spark-env.sh` 文件夹

   之后把整个 `/usr/local/spark` 文件夹压缩分发到各个节点

2. 可从 [http://s198hadoop:7077](http://s198hadoop:7077) 查看 Spark 集群


### 任务一 使用简单移动平均法预测股票涨跌幅（Spark core）
股票数据中共有10只股票的交易信息（Stock文件夹中），每一个文件就是一只股票的信息。本任务随机取其中一只股票作为样本，通过股票的收盘价来建立模型预测股价涨跌幅。
股价的涨跌幅计算中所用到的价格是每天的收盘价，通过比较当日收盘价与前一天的收盘价，计算两日股价差额与前一日收盘价之比来确定股票的涨跌幅度，计算公式如下所示：
（当日收盘价 – 昨日收盘价） / 昨日收盘价 = 涨跌幅
根据以上公式，可以计算出每日的股价涨跌幅，存储在HDFS上。接下来根据以上算出的每日股价涨跌幅数据，使用简单移动平均法预测股价的涨跌幅。简单移动平均法的计算公式如下所示：
 
式中：  ---  对下一期的预测值；
      n  ---   移动平均的时期个数；
         ---  前n期的实际值。
（1）将股票交易信息上传到HDFS，编写一个Spark应用程序，计算出每日股价的涨跌幅数据（需过滤掉收盘价为0的记录），存储于HDFS上。数据包括：日期、涨跌幅值。
（2）编写一个Spark应用程序，根据步骤（1）计算得到的每日涨跌幅数据，使用简单移动平均法预测股价的涨跌幅，预测结果按照年份不同存储于HDFS不同文件上。结果包括：日期、预测涨跌幅值、实际涨跌幅值。[（1）和（2）两个步骤也可以合在一个应用程序里完成，不过分开实现会更清晰]
（3）选取一个年份的股价涨跌幅预测结果，绘出股价的实际涨跌幅与预测涨跌幅对比图。

设计思路：
（1）随机取00009.csv这只股票作为样本，上传到 HDFS上面，由于文件是csv文件，将数据读入rdd后根据“,”进行分割成字符串列表，然后选取第一列和第四列的数据（日期和收盘价）并过滤掉收盘价为0的记录，用队列操作，进行(当日收盘价一昨日收盘价) /昨日收盘价=涨跌幅计算，将计算后的结果存入hdfs中
（2）读入(1)中计算得出的数据，以移动平均的时期个数n为7，利用公式 进行计算预测值,然后与日期，实际涨跌幅进行合并，按年份进行分区后输出。
（3）选取2019年数据用pyecharts绘出预测对比图。


### 任务二 电影评分数据集分析，平均评分最高的 100 部电影，并给出平均评分

采用数据集为 [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/)

![image](https://github.com/wangxi955/HadoopApplicationDevelop/assets/80522598/7cb2bf67-6edb-4091-8245-f3a441d8384a)

具体代码放在 Task02

### 任务三 电商网站评价数据分析

1. 爬取电商网站商品详情页的商品评价数据（数据采集）
2. 清洗电商评价数据（数据清洗）
   对采集到的数据进行清洗，提取出需要的字段，以得到结构化的文本文件
3. 利用 HiveSQL 离线分析评价数据（数据装载、分析）
   使用 Hive 对以下指标进行统计：
   - 统计每天评论数
   - 统计每个评分段（score）的人数，即统计出评 1 分的人数数量；2 分的人数统计；...
4. 利用 Sqoop 进行数据迁移至 MySQL 数据库（数据迁移）
   将 Hadoop 上的分析结果迁移到关系数据库中
5. 完成数据图表展示过程（数据可视化）
   常用可视化工具：JavaWeb + Echarts 或 pyecharts + matplotlib 等
   - 统计每天评论数，给出折线图
   - 统计每个评分段（score）的人数，给出柱状图
6. 编写 Spark 应用程序，利用分词算法（如：IKAnalyzer）分析电商评论数据，对每个分词进行词频统计，统计结果存入 MySQL；利用可视化工具读取 MySQL，给出词云图。

具体代码放在 Task03

### 任务四 ELK (ElasticSearch, Logstash, Kibana) 环境搭建，附上 B 站数据分析

1. 从 Elastic 官网下载 Filebeat、Logstash、Elasticsearch、Kibana 软件，软件版本统一为 7.16.2。
   - 配置 `elasticsearch.yml` 文件
   - 配置 Logstash，在 `/usr/local/logstash/bin` 目录下新建 `logstash1.conf` 文件
   - 配置 `kibana.yml` 文件
2. 通过 Python 采集数据，然后把所采集的数据存入本地文件
