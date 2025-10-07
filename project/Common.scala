import scala.xml.{ Elem => XmlElem, Node => XmlNode }

import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

import com.typesafe.tools.mima.plugin.MimaKeys.mimaFailOnNoPrevious

object Common extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = JvmPlugin

  lazy val actorModule = sys.env.getOrElse("ACTOR_MODULE", "akka")

  val baseSettings = Seq(
    organization := "org.reactivemongo",
    credentials ++= sys.env.get("SONATYPE_USER").toSeq.map { user =>
      Credentials(
        "", // Empty realm credential - this one is actually used by Coursier!
        "central.sonatype.com",
        user,
        Publish.env("SONATYPE_PASS")
      )
    },
    resolvers ++= Seq(
      "Central Testing repository" at "https://central.sonatype.com/api/v1/publisher/deployments/download",
      "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/",
      Resolver.typesafeRepo("releases")
    ),
    mimaFailOnNoPrevious := false,
    Test / logBuffered := false
  )

  val filter = { (ms: Seq[(File, String)]) =>
    ms.filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") &&
        !path.startsWith("samples")
    }
  }

  private val java8 = scala.util.Properties.isJavaAtLeast("1.8")

  val scala211 = "2.11.12"
  val scala212 = "2.12.20"
  val scala213 = "2.13.17"
  val scala3Lts = "3.3.6"

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

  lazy val scalaVersions: Seq[String] = {
    if (actorModule == "akka") Seq(scala211, scala212, scala213, scala3Lts)
    else Seq(scala212, scala213, scala3Lts)
  }

  override def projectSettings =
    Defaults.coreDefaultSettings ++ baseSettings ++ Compiler.settings ++ Seq(
      scalaVersion := scala212,
      crossScalaVersions := scalaVersions,
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
          else "noshaded"
        }

        if (suffix.isEmpty) {
          ver
        } else {
          ver.span(_ != '-') match {
            case (_, "") => s"${ver}-${suffix}"

            case (a, b) => s"${a}-${suffix}.${b stripPrefix "-"}"
          }
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
    ) ++ Publish.settings ++ Publish.mimaSettings ++ new Documentation().settings
}
