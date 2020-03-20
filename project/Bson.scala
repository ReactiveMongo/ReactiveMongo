import sbt._
import sbt.Keys._

class Bson() {
  import Dependencies._

  val discipline = Def.setting[ModuleID] {
    "org.typelevel" %% "discipline-specs2" % "1.1.0"
  }

  val spireLawsVer = Def.setting[String]("0.17.0-M1")

  lazy val module = Project("ReactiveMongo-BSON", file("bson")).
    settings(Findbugs.settings ++ Seq(
      libraryDependencies ++= shaded.value ++ Seq(
        specs.value,
        "org.specs2" %% "specs2-scalacheck" % specsVer.value % Test,
        discipline.value % Test,
        "org.typelevel" %% "spire-laws" % spireLawsVer.value % Test)
    ))
}
