import sbt._
import sbt.Keys._

import com.github.sbt.cpd.CpdPlugin

class Bson() {
  import Dependencies._

  val discipline = Def.setting[ModuleID] {
    if (scalaBinaryVersion.value == "2.10") {
      "org.typelevel" %% "discipline" % "0.9.0"
    } else {
      "org.typelevel" %% "discipline-specs2" % "1.0.0"
    }
  }

  val spireLawsVer = Def.setting[String] {
    if (scalaBinaryVersion.value == "2.10") "0.15.0"
    else "0.17.0-M1"
  }

  lazy val module = Project("ReactiveMongo-BSON", file("bson")).
    enablePlugins(CpdPlugin).
    settings(Findbugs.settings ++ Seq(
      libraryDependencies ++= shaded.value ++ Seq(
        specs.value,
        "org.specs2" %% "specs2-scalacheck" % specsVer.value % Test,
        discipline.value % Test,
        "org.typelevel" %% "spire-laws" % spireLawsVer.value % Test)
    ))
}
