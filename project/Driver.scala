import scala.xml.{
  Attribute => XmlAttr,
  Elem => XmlElem,
  Node => XmlNode,
  NodeSeq,
  XML
}

import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaBinaryIssueFilters

final class Driver(core: Project) {
  import Dependencies._
  import XmlUtil._

  lazy val module = Project("ReactiveMongo", file("driver")).
    settings(Seq(
        description := "ReactiveMongo is a Scala driver that provides fully non-blocking and asynchronous I/O operations ",
        unmanagedSourceDirectories in Compile ++= {
          val v = scalaBinaryVersion.value

          if (v == "2.11" || v == "2.12") {
            Seq((sourceDirectory in Compile).value / "scala-2.11_12")
          } else {
            Seq.empty[File]
          }
        },
        sourceGenerators in Compile += Def.task {
          val ver = version.value
          val dir = (sourceManaged in Compile).value
          val outdir = dir / "reactivemongo" / "api"
          val f = outdir / "Version.scala"
          val major = Common.majorVersion.value

          outdir.mkdirs()

          Seq(IO.writer[File](f, "", IO.defaultCharset, false) { w =>
            w.append(s"""package reactivemongo.api
object Version {
  /** The ReactiveMongo API version */
  override val toString = "$ver"

  /** The major version (e.g. 0.12 for the release 0.12.0) */
  val majorVersion = "${major}"

  /** The Scala major version (e.g. 2.12) */
  val scalaBinaryVersion = "${scalaBinaryVersion.value}"
}""")

            f
          })
        }.taskValue,
        driverCleanup := {
          val classDir = (classDirectory in Compile).value
          val extDir = {
            val d = target.value / "external" / "reactivemongo"
            d.mkdirs(); d
          }

          val classFile = "StaticListenerBinder.class"
          val listenerClass = classDir / "external" / "reactivemongo" / classFile

          streams.value.log(s"Cleanup $listenerClass ...")

          IO.move(listenerClass, extDir / classFile)
        },
        driverCleanup := driverCleanup.triggeredBy(compile in Compile).value,
        libraryDependencies ++= {
          if (!Common.useShaded.value) {
            Seq(Dependencies.netty % Provided)
          } else {
            Seq.empty[ModuleID]
          }
        },
        libraryDependencies ++= akka.value ++ Seq(
          "dnsjava" % "dnsjava" % "3.3.0",
          commonsCodec,
          shapelessTest % Test, specs.value) ++ logApi,
        mimaBinaryIssueFilters ++= {
          //import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

          Seq.empty
        },
        Common.closeableObject in Test := "tests.Common$",
        testOptions in Test += Tests.Cleanup(Common.cleanup.value),
        mappings in (Compile, packageBin) ~= driverFilter,
        //mappings in (Compile, packageDoc) ~= driverFilter,
        mappings in (Compile, packageSrc) ~= driverFilter,
        apiMappings ++= Documentation.mappings("com.typesafe.akka", "http://doc.akka.io/api/akka/%s/")("akka-actor").value ++ Documentation.mappings("com.typesafe.play", "http://playframework.com/documentation/%s/api/scala/index.html", _.replaceAll("[\\d]$", "x"))("play-iteratees").value,
    )).configure { p =>
      sys.props.get("test.nettyNativeArch") match {
        case Some("osx") => p.settings(Seq(
          libraryDependencies += shadedNative("osx-x86-64").value % Test
        ))

        case Some(_/* linux */) => p.settings(Seq(
          libraryDependencies += shadedNative("linux-x86-64").value % Test
        ))

        case _ => p
      }
    }.dependsOn(core)

  // ---

  private def shadedNative(arch: String) = Def.setting[ModuleID] {
    if (Common.useShaded.value) {
      val v = version.value
      val s = {
        if (v endsWith "-SNAPSHOT") {
          s"${v.dropRight(9)}-${arch}-SNAPSHOT"
        } else {
          s"${v}-${arch}"
        }
      }

      organization.value % "reactivemongo-shaded-native" % s
    } else {
      val variant = if (arch == "osx-x86-64") "kqueue" else "epoll"
      val classifier = if (arch == "osx-x86-64") "osx-x86_64" else "linux-x86_64"

      ("io.netty" % s"netty-transport-native-${variant}" % Dependencies.nettyVer).classifier(classifier)
    }
  }

  private val driverFilter: Seq[(File, String)] => Seq[(File, String)] = {
    (_: Seq[(File, String)]).filter {
      case (file, name) =>
        !(name endsWith "external/reactivemongo/StaticListenerBinder.class")
    }
  } andThen Common.filter

  private val driverCleanup = taskKey[Unit]("Driver compilation cleanup")
}
