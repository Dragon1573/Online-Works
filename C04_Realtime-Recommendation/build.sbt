// 配置项目基础设置
name := "Realtime-Recommendation"
version := "0.1"

// 配置Scala基础设置
scalaVersion := "2.11.12"
resolvers += "central" at "https://mirrors.huaweicloud.com/repository/maven/"
scalaSource in Compile := baseDirectory.value / "src"
