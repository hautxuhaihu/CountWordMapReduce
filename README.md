# CountWordMapReduce
jar包分别为IKAnalyzer2012.jar和hadoop的jar包
## 本项目是一个练习mapReduce的demo，结构分为三部分。
### 第一部分
通过api将本地机器的数据上传到hdfs上，以便可以利用hadoop来计算。
### 第二部分：
将hdfs中的文件读入，利用语言分析将**.txt的语言进行分析，获得每个词出现的频率，然后将结果保存到hdfs中的其他位置。
### 第三部分：
将第二部分的结果文件读入，然后继续排序获得频率出现最高的三个频率，并保存到hdfs中的文件中。
