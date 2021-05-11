import sbt._
import sbt.Keys._

object Dependencies {
  val nettyVer = "4.1.51.Final"
  val netty = "io.netty" % "netty-handler" % nettyVer

  val shaded = Def.setting[Seq[ModuleID]] {
    val v = (version in ThisBuild).value

    if (Common.useShaded.value) {
      Seq(organization.value % "reactivemongo-shaded" % v)
    } else {
      Seq(
        netty % Provided,
        "org.reactivemongo" %% "reactivemongo-alias" % v)
    }
  }

  val akka = Def.setting[Seq[ModuleID]] {
    val ver = sys.env.get("AKKA_VERSION").getOrElse {
      val v = scalaBinaryVersion.value

      if (v == "2.12" || v == "2.13") "2.5.25"
      else "2.3.13"
    }

    Seq(
      "com.typesafe.akka" %% "akka-actor" % ver,
      "com.typesafe.akka" %% "akka-testkit" % ver % Test,
      "com.typesafe.akka" %% "akka-slf4j" % ver % Test)
  }

  val specsVer = Def.setting[String] {
    "4.5.1"/*"4.3.5"*/
  }

  val specs = Def.setting[ModuleID] {
    "org.specs2" %% "specs2-core" % specsVer.value % Test
  }

  val slf4jVer = "1.7.30"
  val log4jVer = "2.14.1"

  val slf4j = "org.slf4j" % "slf4j-api" % slf4jVer
  val slf4jSimple = "org.slf4j" % "slf4j-simple" % slf4jVer

  val logApi = Seq(
    slf4j % Provided,
    "org.apache.logging.log4j" % "log4j-api" % log4jVer, // deprecated
    "com.lmax" % "disruptor" % "3.4.4" % Test
  ) ++ Seq("log4j-core", "log4j-slf4j-impl").map(
    "org.apache.logging.log4j" % _ % log4jVer % Test)

  val shapelessTest = "com.chuusai" %% "shapeless" % "2.3.6"

  val commonsCodec = "commons-codec" % "commons-codec" % "1.15"
}
