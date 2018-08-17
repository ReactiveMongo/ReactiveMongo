import sbt._
import sbt.Keys._

object Compiler {
  val settings = Seq(
    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-numeric-widen",
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",
      "-g:vars"
    ),
    scalacOptions in Compile ++= {
      if (!scalaVersion.value.startsWith("2.11.")) Nil
      else Seq(
        "-Yconst-opt",
        "-Yclosure-elim",
        "-Ydead-code",
        "-Yopt:_"
      )
    },
    scalacOptions in Compile ++= {
      if (scalaVersion.value startsWith "2.10.") Nil
      else Seq(
        "-Ywarn-infer-any",
        "-Ywarn-unused",
        "-Ywarn-unused-import",
        "-Xlint:missing-interpolator"
      )
    },
    scalacOptions in Compile ++= {
      if (!scalaVersion.value.startsWith("2.12.")) Seq("-target:jvm-1.6")
      else Seq("-target:jvm-1.8")
    },
    scalacOptions in (Compile, console) ~= {
      _.filterNot { opt => opt.startsWith("-X") || opt.startsWith("-Y") }
    },
    scalacOptions in (Test, console) ~= {
      _.filterNot { opt => opt.startsWith("-X") || opt.startsWith("-Y") }
    },
    scalacOptions in (Test, console) += "-Yrepl-class-based",
    scalacOptions in Compile := {
      val opts = (scalacOptions in Compile).value

      if (scalaVersion.value != "2.10.7") opts
      else {
        opts.filter(_ != "-Ywarn-unused-import")
      }
    }
  )
}
