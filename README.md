# 1. 需求分析
## 1.1 	项目概述
通过提供一个基于大数据平台的天气分析系统。使其对大量的历史天气数据进行存储、管理和分析。提供快速的查询和报告功能，以支持气象研究应用。数据来源为爬取2345天气王的北京市所有区县的历史天气。集成现代数据可视化库，提供交互式的图表，包括折线图、柱状图、饼图等，以展示不同天气指标的时间序列分析。支持用户自定义查询日期与范围。
## 1.2 	项目需求说明
在数据采集功能中，通过自主设计并实现多线程的爬虫程序，同时多线程的定期从2345天气王爬取北京市所有区县的历史天气数据，爬虫采用多线程技术，提高数据抓取的效率。每个线程负责抓取特定区县的天气数据，同时运行多个线程以并行处理。同时存储数据利用大数据存储解决方案，存储原始和加工后的天气数据，如存入MySQL数据库当中。实现数据的备份和恢复机制，确保数据的持久化。
在数据处理与分析功能当中，利用Hive与Spark大数据处理工具，进行数据清洗、转换和聚合，去除无效或重复的数据记录与空缺值的填补。使用pyspark实现将数据清洗功能提交至Hadoop集群中进行大数据的分布式计算、清洗与分析数据等。提供界面，允许用户执行自定义的数据分析。加工后的数据被存储到MySQL数据库中，便于进行SQL查询和 joins 操作，同时支持数据分析。
在可视化互动大屏功能当中，提供一个易于使用的Web界面，具备灵活的查询选项，用户可以按照不同年份、月份、天数来查询数据，如同时选择多个区域和时间段。允许用户自定义查询日期与范围，支持多种查询条件组合。同时具备数据可视化功能，提供交互式图表，包括折线图、柱状图、饼图等。除了基本的图表类型，还提供了北京市天气温度地图，用户可以通过地图直观地比较不同区域的温度变化。以展示不同天气指标的时间序列分析。实现图表的动态更新，以响应用户查询的变化。图表支持交互式探索，用户可以通过点击、拖动等操作深入了解与获取数据。
# 2.	概要设计
## 2.1 	功能综述
该数据平台可以实现对历史天气数据信息的爬取与存储，含有数据清洗、数据分析，数据呈现等功能，使用hive、mysql、python对数据进行数据清洗，streamlit、mysql实现数据呈现的UI框架搭建与展示。
## 2.2 	系统功能层次模块设计
数据采集：通过发送请求后获取页面文本，在爬取过程中创建多个线程，每个线程负责一个地区的天气数据抓取和插入操作。使用pymysql库将提取的数据插入到MySQL数据库中。
数据清洗：通过实现了从MySQL到Hive的数据迁移，并在Spark集群上进行数据的分布式处理和清洗。利用PySpark将处理代码提交到Spark集群。
数据分析：用户可以通过添加限制条件，例如地区、时间、查询的天气数据（例如气温、气候、空气质量等）进行查询， 查询的日期由用户自身输入。
数据可视化：使用python连通mysql数据库，使用sql语句，根据用户需求进行数据选择、分析，选择分析方式进行可视化，以图的形式直观的呈现给用户。
# 3. 环境配置要求
#### Win10系统所用程序版本：
VMware Workstation 16 Pro
Navicat Premium 15
MySQL 8.0.29
Python-3.9.7
#### Centos7系统所用程序版本：
Java Development Kit-8u151 \n
Hadoop-3.2.0 \n
Scala-2.12.10 \n
Hive-3.1.2 \n
Sqoop-1.4.7 \n
Python-3.9.0 \n
mysql-connector-java-8.0.29.jar \n
commons-lang-2.6.jar \n
guava-14.0.1.jar \n
