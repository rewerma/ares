# 本地部署

## 步骤 1: 准备工作

在开始本地运行前，您需要确保您已经安装了Ares所需要的以下软件：

* 安装[Java](https://www.java.com/en/download/) (Java 8 或 11， 其他高于Java 8的版本理论上也可以工作) 以及设置 `JAVA_HOME`。
* 安装[Spark](https://spark.apache.org/downloads.html) (Spark 2.4.3 或 高于3.0.0的版本) 以及设置 `SPARK_HOME`。

## 步骤 2: 构建Ares

下载Ares代码并通过Maven构建，在ares-dist/target目录下，可以找到构建好的ares-bin.tar.gz文件。

## 步骤 3: 安装配置Ares

解压ares-bin.tar.gz文件，进入ares-bin目录，进入connectors目录，将不需要的插件删除（目前Ares connectors支持Jdbc connector），然后将需要的插件拷贝到connectors目录下。

## 步骤 4: 开发Ares-PL/SQL脚本

您可以按照[快速开始](quick-start-ares.md)来配置开发脚本文件。

## 步骤 5: 运行Ares任务

进入ares-bin目录，运行如下命令：

``` bash
./bin/start-ares-spark3-connector.sh --sql /path/to/sample.sql --master spark://127.0.0.1:7077 
``` 

您可以按照[快速开始](quick-start-ares.md)来运行任务。