import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.{
  mimaBinaryIssueFilters,
  mimaPreviousArtifacts
}

import com.github.sbt.cpd.CpdPlugin

import sbtassembly.AssemblyKeys, AssemblyKeys._

class Bson(shaded: Project) {
  import Dependencies._
  import XmlUtil._

  lazy val module = Project("ReactiveMongo-BSON", file("bson")).
    enablePlugins(CpdPlugin).
    settings(Common.settings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(specs.value,
        "org.specs2" %% "specs2-scalacheck" % specsVer.value % Test,
        "org.typelevel" %% "discipline" % "0.9.0" % Test,
        "org.typelevel" %% "spire-laws" % "0.15.0" % Test),
      compile in Compile := (compile in Compile).
        dependsOn(assembly in shaded).value,
      unmanagedJars in Compile := {
        val dir = (target in shaded).value
        val jar = (assemblyJarName in (shaded, assembly)).value

        (dir / "classes").mkdirs() // Findbugs workaround

        Seq(Attributed(dir / jar)(AttributeMap.empty))
      },
      mimaBinaryIssueFilters ++= {
        import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

        @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
        @inline def mtp(s: String) = x[MissingTypesProblem](s)
        @inline def fmp(s: String) = x[FinalMethodProblem](s)
        @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)

        Seq(
          mtp("reactivemongo.bson.BSONDBPointer$"),
          mtp("reactivemongo.bson.BSONDBPointer"),
          mtp("reactivemongo.bson.BSONTimestamp$"),
          mtp("reactivemongo.bson.ExtendedNumeric"),
          fmp("reactivemongo.bson.ExtendedNumeric.value"),
          imt("reactivemongo.bson.ExtendedNumeric.this"),
          mtp("reactivemongo.bson.ExtendedNumeric$"),
          x[UpdateForwarderBodyProblem]("reactivemongo.bson.DefaultBSONHandlers.collectionToBSONArrayCollectionWriter"),
          x[UpdateForwarderBodyProblem]("reactivemongo.bson.DefaultBSONHandlers.bsonArrayToCollectionReader")
        )
      }
    )).dependsOn(shaded % Provided)
}
