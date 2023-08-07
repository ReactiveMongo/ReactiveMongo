import sbt._
import sbt.Keys._

object Dependencies {
  val nettyVer = "4.1.85.Final"
  val netty = "io.netty" % "netty-handler" % nettyVer

  val shaded = Def.setting[Seq[ModuleID]] {
    val v = (ThisBuild / version).value

    if (Common.useShaded.value) {
      Seq(organization.value % "reactivemongo-shaded" % v)
    } else {
      Seq(netty % Provided, "org.reactivemongo" %% "reactivemongo-alias" % v)
    }
  }

  val akka = Def.setting[Seq[ModuleID]] {
    val ver = sys.env.get("AKKA_VERSION").getOrElse {
      val v = scalaBinaryVersion.value

      if (v startsWith "3") "2.6.18"
      else if (v == "2.12" || v == "2.13") "2.5.32"
      else "2.3.13"
    }

    Seq(
      "com.typesafe.akka" %% "akka-actor" % ver,
      "com.typesafe.akka" %% "akka-testkit" % ver % Test,
      "com.typesafe.akka" %% "akka-slf4j" % ver % Test
    )
  }

  val pekko = Def.setting[Seq[ModuleID]] {
    val ver = "1.0.1"

    Seq(
      "org.apache.pekko" %% "pekko-actor" % ver,
      "org.apache.pekko" %% "pekko-testkit" % ver % Test,
      "org.apache.pekko" %% "pekko-slf4j" % ver % Test
    )
  }

  val specsVer = Def.setting[String] {
    if (scalaBinaryVersion.value == "2.11") {
      "4.10.6"
    } else {
      "4.19.0"
    }
  }

  val specs = Def.setting[ModuleID] {
    ("org.specs2" %% "specs2-core" % specsVer.value)
      .cross(CrossVersion.for3Use2_13) % Test
  }

  val slf4jVer = "1.7.36"

  val slf4j = "org.slf4j" % "slf4j-api" % slf4jVer
  val slf4jSimple = "org.slf4j" % "slf4j-simple" % slf4jVer

  val logApi = Seq(
    slf4j % Provided,
    "com.lmax" % "disruptor" % "3.4.4" % Test
  )

  val commonsCodec = "commons-codec" % "commons-codec" % "1.16.0"
}
