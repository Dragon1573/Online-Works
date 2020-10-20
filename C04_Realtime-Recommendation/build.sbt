// 全局项目配置
lazy val globalSettings = Seq(
  // 远程仓库相关
  resolvers ++= Seq(
    "huawei" at "https://mirrors.huaweicloud.com/repository/maven",
    "central" at "https://repo1.maven.org/maven2"
    ),

  // 项目信息
  organization := "wang.dragon1573",
  version := "1.0",

  // 编译器相关
  javacOptions ++= Seq("-source", "1.8"),
  scalaVersion := "2.11.12"
  )

lazy val subProjSettings = Seq(
  // 添加项目依赖项
  libraryDependencies ++= Seq(
    // MySQL 数据源连接引擎
    "mysql" % "mysql-connector-java" % "5.1.45",

    // Apache Flink 实时流处理架构
    "org.apache.flink" %% "flink-streaming-scala" % "1.10.1",
    "org.apache.flink" %% "flink-clients" % "1.10.1",
    "org.apache.flink" %% "flink-connector-kafka" % "1.10.1",

    // 日志系统
    "org.slf4j" % "slf4j-log4j12" % "1.7.7",
    "log4j" % "log4j" % "1.2.17"
    ),

  // 配置Scala基础设置
  scalaSource in Compile := baseDirectory.value / "scala",
  resourceDirectory in Compile := baseDirectory.value / "resources"
  )

// 根项目配置
lazy val baseProj = (project in file(".")).settings(
  globalSettings,
  name := "Realtime-Recommendation"
  ).aggregate(proj_1)

/* SBT 多项目结构配置 */
lazy val proj_1 = (project in file("src/module4/proj_1")).settings(
  // 继承全局配置
  globalSettings,
  subProjSettings,

  // 项目名
  name := "Volume_Query_Daily",
  mainClass in Compile := Some("task2.flink4kafka.SaleVolume")
  )

lazy val proj_2 = (project in file("src/module4/proj_2")).settings(
  globalSettings,
  subProjSettings,

  name := "Store-Sale-Daily",
  mainClass in Compile := Some("process.StoreSaleJob")
  )

lazy val proj_3 = (project in file("src/module4/proj_3")).settings(
  globalSettings,
  subProjSettings,

  name := "Item_Volume_10_Best",
  mainClass in Compile := Some("process.ItemVolume10Best")
  )

lazy val proj_4 = (project in file("src/module5")).settings(
  globalSettings,
  subProjSettings,

  name := "Recommend_Model_Main",

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "2.4.0",
    "org.apache.spark" %% "spark-mllib" % "2.4.0",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"
    )
  )
