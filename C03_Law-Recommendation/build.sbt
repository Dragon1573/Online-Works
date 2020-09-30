resolvers += "central" at "https://mirrors.huaweicloud.com/repository/maven/"
name := "Law-Recommendation"
version := "1.0"
scalaVersion := "2.11.5"
scalaSource in Compile := baseDirectory.value / "src"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.4.0",
                            "org.apache.spark" %% "spark-mllib" % "2.4.0")
