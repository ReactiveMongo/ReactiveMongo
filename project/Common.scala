import scala.xml.{ Elem => XmlElem, Node => XmlNode }

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

import com.typesafe.tools.mima.plugin.MimaKeys.mimaFailOnNoPrevious

object Common extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = JvmPlugin

  val baseSettings = Seq(
    organization := "org.reactivemongo",
    resolvers += Resolver.typesafeRepo("releases"),
    resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
    resolvers ++= Resolver.sonatypeOssRepos("staging"),
    mimaFailOnNoPrevious := false,
    Test / logBuffered := false
  )

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") &&
        !path.startsWith("samples")
    }
  }

  private val java8 = scala.util.Properties.isJavaAtLeast("1.8")

  val scala211 = "2.11.12"
  val scala213 = "2.13.10"
  val scala31 = "3.2.1

  def majorVersion = {
    val Major = """([0-9]+)\.([0-9]+)\..*""".r

    Def.setting[String] {
      (ThisBuild / version).value match {
        case Major(maj, min) =>
          s"${maj}.${min}"

        case ver =>
          sys.error(s"Invalid version: $ver")
      }
    }
  }

  val useShaded = settingKey[Boolean](
    "Use ReactiveMongo-Shaded (see system property 'reactivemongo.shaded')"
  )

  override def projectSettings =
    Defaults.coreDefaultSettings ++ baseSettings ++ Compiler.settings ++ Seq(
      scalaVersion := "2.12.17",
      crossScalaVersions := Seq(
        scala211,
        scalaVersion.value,
        scala213,
        scala31
      ),
      crossVersion := CrossVersion.binary,
      useShaded := sys.env.get("REACTIVEMONGO_SHADED").fold(true)(_.toBoolean),
      target := {
        if (useShaded.value) target.value / "shaded"
        else target.value / "noshaded"
      },
      version := {
        val ver = (ThisBuild / version).value
        val suffix = {
          if (useShaded.value) "" // default ~> no suffix
          else "-noshaded"
        }

        ver.span(_ != '-') match {
          case (a, b) => s"${a}${suffix}${b}"
        }
      },
      Compile / unmanagedSourceDirectories ++= {
        val jdir = if (java8) "java8" else "java7"

        Seq((Compile / sourceDirectory).value / jdir)
      },
      Test / unmanagedSourceDirectories ++= {
        val jdir = if (java8) "java8" else "java7"

        Seq((Compile / sourceDirectory).value / jdir)
      },
      Compile / packageBin / mappings ~= filter,
      Compile / packageSrc / mappings ~= filter,
      Compile / packageDoc / mappings ~= filter,
      testFrameworks ~= { _.filterNot(_ == TestFrameworks.ScalaTest) }
    ) ++ Publish.settings ++ Publish.mimaSettings
}
