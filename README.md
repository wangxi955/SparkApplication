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

！image(/assets/FlowChart01)
设计思路：
（1）随机取00009.csv这只股票作为样本，上传到 HDFS上面，由于文件是csv文件，将数据读入rdd后根据“,”进行分割成字符串列表，然后选取第一列和第四列的数据（日期和收盘价）并过滤掉收盘价为0的记录，用队列操作，进行(当日收盘价一昨日收盘价) /昨日收盘价=涨跌幅计算，将计算后的结果存入hdfs中
（2）读入(1)中计算得出的数据，以移动平均的时期个数n为7，利用公式 进行计算预测值,然后与日期，实际涨跌幅进行合并，按年份进行分区后输出。
（3）选取2019年数据用pyecharts绘出预测对比图。


### 任务二 构建网站信任网络

（1）构建网站信任网络，找出网站需要支付稿酬的用户。为了鼓励用户点评，所以对于信任度比较高的前50名用户会支付一定的稿酬。想要找出信任度比较高的用户，首先需要计算每个用户的被信任度，也就是计算每个顶点的入度数。计算完入度数之后，需要根据入度数进行排序，排序按照从高到低的顺序，然后从排序后的顶点数据中取出前50名作为奖励用户，这50名用户就是网站需要支付稿酬的用户了。
（2）找出有资格进热门点评榜的用户。热门排行榜是用于对网站用户进行一个排名的，想上排行榜需要满足一定的要求才有资格进入。第一点就是用户的信任度必须在网站用户信任度排名中排在前3%；第二点就是用户的活跃度在满足第一点的用户中排在前5%，其中信任度表示入度数，活跃度表示出度数。根据以上要求，要找出满足条件的用户，第一点需要计算入度数并排列取出前3%的用户，然后再计算这3%的用户的出度数，取出前5%的用户，这部分用户即为可上榜用户。
（3）给用户推荐可信任用户。一个用户对另一个用户表示信任，那么他/她可能对于另一个用户所信任的其他用户的点评同样也比较信任。将信任人的信任人推荐给这个用户，属于二度关系推荐，这是常用的一种最简单的推荐方法。这个过程可以称为二度关系或二跳邻居，二度关系推荐可抽象成在有向图中寻找到指定顶点的最短距离为2的所有顶点。

！image(/assets/FlowChart02)

### 任务三 对爬取到的商品信息进行实时分析（Structured Streaming）

爬取京东上的商品信息，存储到本地文件系统目录（/home/hadoop/structuredstreaming）下，格式为csv文件，包含三个字段（sku，商品名称，价格）。
编写一个Structured Streaming应用程序，实时监控该目录，统计商品价格大于等于100元的商品数量、小于100元的商品数量，输出到控制台。


### 任务四 预测用户是否有意向参加培训（Spark ML）

“泰迪杯”数据挖掘竞赛网站作为一个竞赛型的网站，不仅发布了很多竞赛的相关信息，除此之外还有很多其他类型的网页信息，其中就包括公司主营的业务培训的相关信息。每个网页都属于某一类网页标签，例如“培训”“竞赛”“项目”等，用户对于某个网页的访问可以转化为用户对某类标签的访问记录。现有一份记录了用户对各网页标签的统计数据，如图所示，这份数据包括用户对“泰迪杯”数据挖掘竞赛网站中各个网页标签的访问记录。表中的第1列id，代表用户id。标签列“status”，标识了用户是否访问过培训标签相关的网页。如果访问过，则值为1，表示此用户对于培训有参加的意向；反之其值为0，表示此用户对于培训没有参加的意向。其他的标签列共有30个它们的值各自表示了用户访问该网页标签的访问次数。比如，标签列“项目”的值为3，则表示用户访问项目标签相关的网页共有3次。如果根据标签列“status”进行简单分类，则所有用户被分成了两类：有意向培训用户和无意向培训用户。
这里使用样本数据中的标签列“status”作为模型的目标列，其他30个标签列的数据作为属性列来构建一个分类模型。应用分类模型就可以对用户进行分类，从而推测出新用户是否有培训意向。
！image(/assets/Picture)
先基于以上已有的数据（wget 10.10.246.3:8086/usr/local/upload/files/taidibei.csv），去掉对所有标签都没有访问的用户。然后利用逻辑回归（org.apache.spark.ml.classification.LogisticRegression）算法训练出一个分类器。
将训练出的模型，保存在HDFS上。
编写一个Spark应用程序，加载保存在HDFS上的模型，读取待预测的数据集（wget 10.10.246.3:8086/usr/local/upload/files/taidibei_toTest.csv），预测数据集中每个用户参加培训的意向。

设计思路：
（1）数据处理：使用python语言读取数据集taidibei.csv，并转换为DataFrame，对‘竞赛’到‘农业’这三十个标签列的数据进行切片，遍历行并记录所有标签都没有访问的行序号（即单行总和为0），使用DataFrame的drop函数去除对应行，保存为文件taidibei.csv。
（2）创建LogisticTrainModel对象：读取文件taidibei.csv，并把status列重命名为label列（字符类型转换为整型），将数据集按8:2比例切割为训练集和测试集，使用逻辑回归算法作为评估器，构建Pipeline，包含两个流水线阶段featureHasher和logisticRegression。
（3）模型调优：设置参数网格，正则化参数为0.1,0.2,0.3，最大迭代次数60,80，100，使用交叉验证进行模型调优，并把模型保存到hdfs上。
（4）预测数据集：读取待预测的数据集，并使用调优后的模型进行预测。

