import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaFailOnNoPrevious

object Common {
  val baseSettings = Seq(
    organization := "org.reactivemongo",
    resolvers ++= Seq(
      Resolver.typesafeRepo("releases"),
      Resolver.sonatypeRepo("snapshots")
    ),
    mimaFailOnNoPrevious := false
  )

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") &&
        !path.startsWith("samples")
    }
  }

  private val java8 = scala.util.Properties.isJavaAtLeast("1.8")

  val scalaCompatVer = "2.11.12"

  val closeableObject = SettingKey[String]("class name of a closeable object")

  val settings = Defaults.coreDefaultSettings ++ baseSettings ++ Compiler.settings ++ Seq(
    scalaVersion := "2.12.10",
    crossScalaVersions := Seq(
      "2.10.7", scalaCompatVer, scalaVersion.value, "2.13.1"),
    crossVersion := CrossVersion.binary,
    //parallelExecution in Test := false,
    //fork in Test := true, // Don't share executioncontext between SBT CLI/tests
    unmanagedSourceDirectories in Compile ++= {
      val jdir = if (java8) "java8" else "java7"

      Seq((sourceDirectory in Compile).value / jdir)
    },
    unmanagedSourceDirectories in Test ++= {
      val jdir = if (java8) "java8" else "java7"

      Seq((sourceDirectory in Compile).value / jdir)
    },
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation",
      /*"-diagrams", */"-implicits", "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(Release.major.value),
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter,
    testFrameworks ~= { _.filterNot(_ == TestFrameworks.ScalaTest) },
    closeableObject in Test := "Common$"
  ) ++ Publish.settings ++ Format.settings ++ (
    Release.settings ++ Publish.mimaSettings)

  val cleanup = Def.task[ClassLoader => Unit] {
    val log = streams.value.log

    {cl: ClassLoader =>
      import scala.language.reflectiveCalls

      val objectClass = (closeableObject in Test).value
      val c = cl.loadClass(objectClass)
      type M = { def close(): Unit }
      val m: M = c.getField("MODULE$").get(null).asInstanceOf[M]

      log.info(s"Closing $m ...")

      m.close()
    }
  }
}
