import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.{
  mimaBinaryIssueFilters,
  mimaPreviousArtifacts
}

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
        "org.typelevel" %% "spire-laws" % spireLawsVer.value % Test),
      mimaBinaryIssueFilters ++= {
        import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

        @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
        @inline def mtp(s: String) = x[MissingTypesProblem](s)
        @inline def fmp(s: String) = x[FinalMethodProblem](s)
        @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
        @inline def isp(s: String) = x[IncompatibleSignatureProblem](s)

        val pkg = "reactivemongo.bson"

        Seq(
          isp(/* package private */
            s"${pkg}.buffer.BSONIterator.pretty"),
          isp(s"${pkg}.BSONDocument.++"),
          isp(s"${pkg}.BSONDocument.elements"),
          isp(s"${pkg}.BSONDocument.stream"),
          isp(s"${pkg}.BSONDocument.copy"),
          isp(s"${pkg}.BSONDocument.copy$$default$$1"),
          isp(s"${pkg}.BSONDocument.apply"),
          isp(s"${pkg}.BSONDocument.unapply"),
          isp(s"${pkg}.BSONDocument.this"),
          isp(s"${pkg}.BSONHandler.apply"),
          isp(s"${pkg}.package.document"),
          mtp(s"${pkg}.BSONDBPointer$$"),
          mtp(s"${pkg}.BSONDBPointer"),
          mtp(s"${pkg}.BSONTimestamp$$"),
          mtp(s"${pkg}.ExtendedNumeric"),
          fmp(s"${pkg}.ExtendedNumeric.value"),
          imt(s"${pkg}.ExtendedNumeric.this"),
          mtp(s"${pkg}.ExtendedNumeric$$"),
          x[UpdateForwarderBodyProblem](s"${pkg}.DefaultBSONHandlers.collectionToBSONArrayCollectionWriter"),
          x[UpdateForwarderBodyProblem](s"${pkg}.DefaultBSONHandlers.bsonArrayToCollectionReader")
        )
      }
    ))
}
