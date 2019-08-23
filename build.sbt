import sbt.{Credentials, Path}

name := "hs-sparklens"
organization  := "net.structureit.homestead"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

scalaVersion := "2.11.12"

//crossScalaVersions := Seq("2.10.6", "2.11.8")

//spName := "qubole/sparklens"

publishTo := Some("StuctureIT Releases" at "http://build.structureit.net:8081/nexus/repository/maven-releases")


sparkVersion := "2.4.0"

spAppendScalaVersion := true


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided"

libraryDependencies += "io.prometheus" % "simpleclient_pushgateway" % "0.6.0"

libraryDependencies += "io.prometheus" % "simpleclient" % "0.6.0"

libraryDependencies += "io.prometheus" % "simpleclient_hotspot" % "0.6.0"

libraryDependencies += "io.prometheus" % "simpleclient_httpserver" % "0.6.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" 






testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.7")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

publishMavenStyle := true


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")


pomExtra :=
  <url>https://github.com/qubole/sparklens</url>
  <scm>
    <url>git@github.com:qubole/sparklens.git</url>
    <connection>scm:git:git@github.com:qubole/sparklens.git</connection>
  </scm>
  <developers>
    <developer>
      <id>iamrohit</id>
      <name>Rohit Karlupia</name>
      <url>https://github.com/iamrohit</url>
    </developer>
    <developer>
      <id>beriaanirudh</id>
      <name>Anirudh Beria</name>
      <url>https://github.com/beriaanirudh</url>
    </developer>
    <developer>
      <id>mayurdb</id>
      <name>Mayur Bhosale</name>
      <url>https://github.com/mayurdb</url>
    </developer>
  </developers>

