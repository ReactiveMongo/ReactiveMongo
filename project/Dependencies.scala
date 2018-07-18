import sbt._
import sbt.Keys._

object Resolvers {
  val resolversList = Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
}

object Dependencies {
  val netty = "4.1.26.Final"

  val akka = Def.setting[Seq[ModuleID]] {
    val ver = sys.env.get("AKKA_VERSION").getOrElse {
      if (scalaVersion.value startsWith "2.12.") "2.5.13"
      else "2.3.13"
    }

    Seq(
      "com.typesafe.akka" %% "akka-actor" % ver,
      "com.typesafe.akka" %% "akka-testkit" % ver % Test,
      "com.typesafe.akka" %% "akka-slf4j" % ver % Test)
  }

  val playIteratees = Def.setting[ModuleID] {
    val ver = sys.env.get("ITERATEES_VERSION").getOrElse {
      if (scalaVersion.value startsWith "2.10.") "2.3.9"
      else "2.6.1"
    }

    "com.typesafe.play" %% "play-iteratees" % ver
  }

  val specsVer = Def.setting[String] {
    if (scalaVersion.value startsWith "2.10") "3.9.5" // 4.0.1 not avail
    else "4.2.0"
  }

  val specs = Def.setting[ModuleID] {
    "org.specs2" %% "specs2-core" % specsVer.value % Test
  }

  val slf4jVer = "1.7.21"
  val log4jVer = "2.5"


  val slf4j = "org.slf4j" % "slf4j-api" % slf4jVer

  val logApi = Seq(
    slf4j % "provided",
    "org.apache.logging.log4j" % "log4j-api" % log4jVer % Test,
    "org.apache.logging.log4j" % "log4j-core" % log4jVer % Test,
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVer % Test
  )

  val shapelessTest = "com.chuusai" %% "shapeless" % "2.3.3"

  val commonsCodec = "commons-codec" % "commons-codec" % "1.11"
}
