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
        scalacOptions ++= {
          if (scalaBinaryVersion.value == "3") {
            Seq("-Wconf:cat=deprecation&msg=.*(MongoWireVersion|reflectiveSelectableFromLangReflectiveCalls|right-biased|scheduleAtFixedRate|filterInPlace|AtlasSearch).*:s")
          } else {
            Seq.empty
          }
        },
        Compile / unmanagedSourceDirectories ++= {
          val v = scalaBinaryVersion.value

          if (v == "2.11" || v == "2.12") {
            Seq((Compile / sourceDirectory).value / "scala-2.11_12")
          } else {
            Seq.empty[File]
          }
        },
        Compile / sourceGenerators += Def.task {
          val dir = (Compile / sourceManaged).value

          Seq(
            generateVersion(
              scalaBinVer = scalaBinaryVersion.value,
              ver = version.value,
              major = Common.majorVersion.value,
              dir = dir),
            generateTrace(dir))
        }.taskValue,
        driverCleanup := {
          val classDir = (Compile / classDirectory).value
          val extDir = {
            val d = target.value / "external" / "reactivemongo"
            d.mkdirs(); d
          }

          val classFile = "StaticListenerBinder.class"
          val listenerClass = classDir / "external" / "reactivemongo" / classFile

          streams.value.log(s"Cleanup $listenerClass ...")

          IO.move(listenerClass, extDir / classFile)
        },
        driverCleanup := driverCleanup.triggeredBy(Compile / compile).value,
        libraryDependencies ++= {
          if (!Common.useShaded.value) {
            Seq(Dependencies.netty % Provided)
          } else {
            Seq.empty[ModuleID]
          }
        },
        libraryDependencies ++= akka.value ++ Seq(
          "dnsjava" % "dnsjava" % "3.4.3",
          commonsCodec,
          /* TODO: shapelessTest % Test, */ specs.value) ++ logApi,
        mimaBinaryIssueFilters ++= {
          import com.typesafe.tools.mima.core._

          val mtp = ProblemFilters.exclude[MissingTypesProblem](_)
          val fcp = ProblemFilters.exclude[FinalClassProblem](_)

          Seq(
            mtp("reactivemongo.api.ConnectionState$"),
            mtp("reactivemongo.core.actors.MongoDBSystem$OperationHandler"),
            mtp("reactivemongo.core.netty.ChannelFactory"),
            mtp("reactivemongo.core.protocol.MongoHandler"),
            mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
            mtp("reactivemongo.core.protocol.RequestEncoder"),
            fcp("reactivemongo.core.protocol.Request"),
            mtp("reactivemongo.core.protocol.Request$"),
            fcp("reactivemongo.core.protocol.RequestMaker"),
            mtp("reactivemongo.core.protocol.RequestMaker$")
          )
        },
        Test / testOptions += {
          val log = streams.value.log
          val objectClass = f"tests.Common$$"

          Tests.Cleanup {cl: ClassLoader =>
            import scala.language.reflectiveCalls

            val c = cl.loadClass(objectClass)
            type M = { def close(): Unit }
            val m: M = c.getField("MODULE$").get(null).asInstanceOf[M]

            log.info(s"Closing $m ...")

            m.close()
          }
        },
        Compile / packageBin / mappings ~= driverFilter,
        //mappings in (Compile, packageDoc) ~= driverFilter,
        Compile / packageSrc / mappings ~= driverFilter,
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

  private def generateVersion(
    scalaBinVer: String,
    ver: String,
    major: String,
    dir: File
  ): File = {
    val outdir = dir / "reactivemongo" / "api"
    val f = outdir / "Version.scala"

    outdir.mkdirs()

    IO.writer[File](f, "", IO.defaultCharset, false) { w =>
      w.append(s"""package reactivemongo.api
object Version {
  /** The ReactiveMongo API version */
  override val toString = "$ver"

  /** The major version (e.g. 0.12 for the release 0.12.0) */
  val majorVersion = "${major}"

  /** The Scala major version (e.g. 2.12) */
  val scalaBinaryVersion = "${scalaBinVer}"
}""")

      f
    }
  }

  private def generateTrace(dir: File): File = {
    val outdir = dir / "reactivemongo" / "util"
    val f = outdir / "Trace.scala"

    val collect: String =
      sys.props.get("reactivemongo.collectThreadTrace") match {
        case Some("true") =>
          "def currentTraceElements = Thread.currentThread.getStackTrace.toSeq"

        case _ =>
          "val currentTraceElements = Seq.empty[StackTraceElement]"
      }

    outdir.mkdirs()

    IO.writer[File](f, "", IO.defaultCharset, false) { w =>
      w.append(s"""package reactivemongo.util
private[reactivemongo] object Trace {
  $collect
}""")

      f
    }
  }

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
