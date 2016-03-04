import sbt._
import sbt.Keys._
import scala.language.postfixOps

object BuildSettings {
  val buildVersion = "0.12.0-SNAPSHOT-ADENDA"

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") && !path.startsWith("samples")
    }
  }

  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "org.reactivemongo",
    version := buildVersion,
    scalaVersion := "2.11.7",
    crossScalaVersions := Seq("2.11.7", "2.10.4"),
    crossVersion := CrossVersion.binary,
    javaOptions in test ++= Seq("-Xmx512m", "-XX:MaxPermSize=512m"),
    //fork in Test := true, // Don't share executioncontext between SBT CLI/tests
    scalacOptions in Compile ++= Seq(
      "-unchecked", "-deprecation", "-target:jvm-1.6", "-Ywarn-unused-import"),
    scalacOptions in (Compile, doc) ++= Seq(
      "-unchecked", "-deprecation", "-diagrams", "-implicits",
      "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(buildVersion),
    scalacOptions in Compile := {
      val opts = (scalacOptions in Compile).value

      if (scalaVersion.value != "2.11.7") {
        opts.filter(_ != "-Ywarn-unused-import")
      } else opts
    },
    shellPrompt := ShellPrompt.buildShellPrompt,
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter) ++
  Publish.settings ++ Format.settings ++ Publish.mimaSettings

}

object Publish {
  import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
  import com.typesafe.tools.mima.plugin.MimaKeys.{
    binaryIssueFilters, previousArtifacts
  }
  import com.typesafe.tools.mima.core._, ProblemFilters._, Problem.ClassVersion

  @inline def env(n: String): String = sys.env.get(n).getOrElse(n)

  private val repoName = env("PUBLISH_REPO_NAME")
  private val repoUrl = env("PUBLISH_REPO_URL")

  val previousVersion = "0.11.0"

  val missingMethodInOld: ProblemFilter = {
    case mmp @ MissingMethodProblem(_) if (
      mmp.affectedVersion == ClassVersion.Old) => false

    case _ => true
  }  

  val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifacts := {
      if (crossPaths.value) {
        Set(organization.value % s"${moduleName.value}_${scalaBinaryVersion.value}" % previousVersion)
      } else {
        Set(organization.value % moduleName.value % previousVersion)
      }
    },
    binaryIssueFilters ++= Seq(missingMethodInOld))

  lazy val settings = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo := Some(repoUrl).map(repoName at _),
    credentials += Credentials(repoName, env("PUBLISH_REPO_ID"),
        env("PUBLISH_USER"), env("PUBLISH_PASS")),
    pomIncludeRepository := { _ => false },
    licenses := {
      Seq("Apache 2.0" ->
        url("http://www.apache.org/licenses/LICENSE-2.0"))
    },
    homepage := Some(url("http://reactivemongo.org")),
    pomExtra := (
      <scm>
        <url>git://github.com/ReactiveMongo/ReactiveMongo.git</url>
        <connection>scm:git://github.com/ReactiveMongo/ReactiveMongo.git</connection>
      </scm>
      <developers>
        <developer>
          <id>sgodbillon</id>
          <name>Stephane Godbillon</name>
          <url>http://stephane.godbillon.com</url>
        </developer>
      </developers>))
}

object Format {
  import com.typesafe.sbt.SbtScalariform._

  lazy val settings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := formattingPreferences)

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, true).
      setPreference(AlignSingleLineCaseStatements, true).
      setPreference(CompactControlReadability, false).
      setPreference(CompactStringConcatenation, false).
      setPreference(DoubleIndentClassDeclaration, true).
      setPreference(FormatXml, true).
      setPreference(IndentLocalDefs, false).
      setPreference(IndentPackageBlocks, true).
      setPreference(IndentSpaces, 2).
      setPreference(MultilineScaladocCommentsStartOnFirstLine, false).
      setPreference(PreserveSpaceBeforeArguments, false).
      setPreference(PreserveDanglingCloseParenthesis, false).
      setPreference(RewriteArrowSymbols, false).
      setPreference(SpaceBeforeColon, false).
      setPreference(SpaceInsideBrackets, false).
      setPreference(SpacesWithinPatternBinders, true)
  }
}

// Shell prompt which show the current project,
// git branch and build version
object ShellPrompt {
  object devnull extends ProcessLogger {
    def info(s: => String) {}

    def error(s: => String) {}

    def buffer[T](f: => T): T = f
  }

  def currBranch = (
    ("git status -sb" lines_! devnull headOption)
    getOrElse "-" stripPrefix "## ")

  val buildShellPrompt = {
    (state: State) =>
      {
        val currProject = Project.extract(state).currentProject.id
        "%s:%s:%s> ".format(
          currProject, currBranch, BuildSettings.buildVersion)
      }
  }
}

object Resolvers {
  val typesafe = Seq(
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/")
  val resolversList = typesafe
}

object Dependencies {
  val netty = "io.netty" % "netty" % "3.10.4.Final" cross CrossVersion.Disabled

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.6"

  val iteratees = "com.typesafe.play" %% "play-iteratees" % "2.3.10"

  val specs = "org.specs2" %% "specs2-core" % "2.4.9" % Test

  val logApiVersion = "1.7.12"
  val logApi = Seq(
    "org.slf4j" % "slf4j-api" % logApiVersion % "provided",
    "org.apache.logging.log4j" % "log4j-api" % "2.0.2" // deprecated
  )

  val shapelessTest = "com.chuusai" % "shapeless" % "2.0.0" %
  Test cross CrossVersion.binaryMapped {
    case "2.10" => "2.10.4"
    case x => x
  }

  val commonsCodec = "commons-codec" % "commons-codec" % "1.10"
}

object ReactiveMongoBuild extends Build {
  import BuildSettings._
  import Resolvers._
  import Dependencies._
  import sbtunidoc.{ Plugin => UnidocPlugin }
  import com.typesafe.tools.mima.core._, ProblemFilters._, Problem.ClassVersion
  import com.typesafe.tools.mima.plugin.MimaKeys.{
    binaryIssueFilters, previousArtifacts
  }

  val projectPrefix = "ReactiveMongo"

  lazy val reactivemongo =
    Project(
      s"$projectPrefix-Root",
      file("."),
      settings = buildSettings ++ (publishArtifact := false) ).
      settings(UnidocPlugin.unidocSettings: _*).
      settings(previousArtifacts := Set.empty).
    aggregate(driver, bson, bsonmacros)

  lazy val driver = Project(
    projectPrefix,
    file("driver"),
    settings = buildSettings ++ Seq(
      resolvers := resolversList,
      libraryDependencies ++= Seq(
        netty,
        akkaActor,
        iteratees,
        commonsCodec,
        shapelessTest,
        specs) ++ logApi,
      binaryIssueFilters ++= {
        import ProblemFilters.{ exclude => x }
        @inline def mmp(s: String) = x[MissingMethodProblem](s)
        @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
        @inline def fmp(s: String) = x[FinalMethodProblem](s)
        @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
        @inline def mtp(s: String) = x[MissingTypesProblem](s)
        @inline def mcp(s: String) = x[MissingClassProblem](s)

        Seq(
          x[IncompatibleTemplateDefProblem](
            "reactivemongo.core.actors.MongoDBSystem"),
          mcp("reactivemongo.core.actors.RequestIds"),
          mmp("reactivemongo.api.CollectionMetaCommands.drop"),
          mmp("reactivemongo.api.DB.coll"),
          mmp("reactivemongo.api.DB.coll$default$2"),
          mmp("reactivemongo.api.DB.defaultReadPreference"),
          mmp("reactivemongo.api.DB.coll$default$4"),
          mmp("reactivemongo.api.DBMetaCommands.serverStatus"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.DistinctResultReader"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.AggregationFramework"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.FindAndModifyReader"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.DistinctWriter"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.FindAndModifyCommand"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.AggregateWriter"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.DistinctCommand"),
          mmp(
            "reactivemongo.api.collections.BatchCommands.AggregateReader"),
          irt("reactivemongo.api.commands.Upserted._id"),
          imt("reactivemongo.api.commands.Upserted.this"),
          imt("reactivemongo.api.commands.Upserted.copy"),
          irt("reactivemongo.api.Cursor.logger"),
          mcp("reactivemongo.api.commands.tst2"),
          mcp("reactivemongo.api.commands.tst2$"),
          mtp("reactivemongo.api.commands.DefaultWriteResult"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.fillInStackTrace"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.isUnauthorized"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.getMessage"),
          irt("reactivemongo.api.commands.DefaultWriteResult.originalDocument"),
          irt("reactivemongo.core.commands.Getnonce.ResultMaker"),
          irt("reactivemongo.core.protocol.RequestEncoder.logger"),
          mmp("reactivemongo.api.MongoConnection#MonitorActor.killed_="),
          mmp("reactivemongo.api.MongoConnection#MonitorActor.primaryAvailable_="),
          mmp("reactivemongo.api.MongoConnection#MonitorActor.killed"),
          mmp(
            "reactivemongo.api.MongoConnection#MonitorActor.primaryAvailable"),
          mmp("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand._debug"),
          fmp(
            "reactivemongo.api.commands.AggregationFramework#Project.toString"),
          fmp(
            "reactivemongo.api.commands.AggregationFramework#Redact.toString"),
          fmp("reactivemongo.api.commands.AggregationFramework#Sort.toString"),
          mmp("reactivemongo.api.commands.AggregationFramework#Limit.n"),
          mmp("reactivemongo.api.commands.AggregationFramework#Limit.name"),
          mtp("reactivemongo.api.commands.AggregationFramework$Limit"),
          irt("reactivemongo.api.gridfs.DefaultFileToSave.filename"),
          irt("reactivemongo.api.gridfs.DefaultReadFile.filename"),
          irt("reactivemongo.api.gridfs.DefaultReadFile.length"),
          irt("reactivemongo.api.gridfs.BasicMetadata.filename"),
          irt("reactivemongo.api.gridfs.package.logger"),
          mtp("reactivemongo.api.MongoConnectionOptions$"),
          mmp("reactivemongo.api.MongoConnectionOptions.apply"),
          mmp("reactivemongo.api.MongoConnectionOptions.copy"),
          mmp("reactivemongo.api.MongoConnectionOptions.this"),
          fmp("reactivemongo.api.commands.AggregationFramework#Group.toString"),
          mcp("reactivemongo.api.commands.tst2$Toto"),
          fmp("reactivemongo.api.commands.AggregationFramework#Match.toString"),
          mmp("reactivemongo.api.commands.WriteResult.originalDocument"),
          fmp(
            "reactivemongo.api.commands.AggregationFramework#GeoNear.toString"),
          irt("reactivemongo.api.gridfs.ComputedMetadata.length"),
          irt("reactivemongo.core.commands.Authenticate.ResultMaker"),
          mtp("reactivemongo.api.gridfs.DefaultFileToSave"),
          mmp("reactivemongo.api.gridfs.DefaultFileToSave.productElement"),
          mmp("reactivemongo.api.gridfs.DefaultFileToSave.productArity"),
          mmp("reactivemongo.api.gridfs.DefaultFileToSave.productIterator"),
          mmp("reactivemongo.api.gridfs.DefaultFileToSave.productPrefix"),
          imt("reactivemongo.api.gridfs.DefaultFileToSave.this"),
          mtp("reactivemongo.api.gridfs.DefaultFileToSave$"),
          imt("reactivemongo.api.gridfs.DefaultReadFile.apply"),
          imt("reactivemongo.api.gridfs.DefaultReadFile.copy"),
          imt("reactivemongo.api.gridfs.DefaultReadFile.this"),
          mmp("reactivemongo.api.gridfs.DefaultFileToSave.apply"),
          imt("reactivemongo.api.gridfs.DefaultFileToSave.copy"),
          mmp("reactivemongo.api.commands.AggregationFramework#Skip.n"),
          mmp("reactivemongo.api.commands.AggregationFramework#Skip.name"),
          mtp("reactivemongo.api.commands.AggregationFramework$Skip"),
          mcp("reactivemongo.api.commands.AggregationFramework$PipelineStage"),
          mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.needsCursor"),
          mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.cursorOptions"),
          mtp("reactivemongo.api.commands.WriteResult"),
          mtp("reactivemongo.api.commands.UpdateWriteResult"),
          mmp("reactivemongo.api.commands.UpdateWriteResult.fillInStackTrace"),
          mmp("reactivemongo.api.commands.UpdateWriteResult.isUnauthorized"),
          mmp("reactivemongo.api.commands.UpdateWriteResult.getMessage"),
          mmp(
            "reactivemongo.api.commands.UpdateWriteResult.isNotAPrimaryError"),
          irt("reactivemongo.api.commands.UpdateWriteResult.originalDocument"),
          mtp("reactivemongo.api.commands.AggregationFramework$Match$"),
          mtp("reactivemongo.api.commands.AggregationFramework$Redact$"),
          mcp("reactivemongo.api.commands.AggregationFramework$DocumentStageCompanion"),
          mtp("reactivemongo.api.commands.AggregationFramework$Project$"),
          mtp("reactivemongo.api.commands.AggregationFramework$Sort$"),
          mtp("reactivemongo.core.commands.Authenticate$"),
          mmp("reactivemongo.api.commands.AggregationFramework#Unwind.prefixedField"),
          mmp("reactivemongo.core.commands.Authenticate.apply"),
          mmp("reactivemongo.core.commands.Authenticate.apply"),
          mtp("reactivemongo.api.commands.LastError$"),
          mmp("reactivemongo.api.commands.LastError.apply"),
          irt("reactivemongo.api.commands.LastError.originalDocument"),
          mmp("reactivemongo.api.commands.LastError.copy"),
          mmp("reactivemongo.api.commands.LastError.this"),
          mtp("reactivemongo.api.commands.CollStatsResult$"),
          mmp("reactivemongo.api.commands.CollStatsResult.apply"),
          mmp("reactivemongo.api.commands.CollStatsResult.this"),
          mmp("reactivemongo.api.commands.GetLastError#TagSet.s"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.apply"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.this"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.copy"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.copy"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
          mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.apply"),
          irt("reactivemongo.api.commands.LastError.originalDocument"),
          imt("reactivemongo.api.commands.AggregationFramework#Group.apply"),
          mmp("reactivemongo.api.commands.AggregationFramework#Redact.apply"),
          imt("reactivemongo.api.commands.Upserted.apply"),
          mmp("reactivemongo.api.commands.AggregationFramework#Match.apply"),
          irt("reactivemongo.api.commands.AggregationFramework#Sort.apply"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.apply"),
          mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.apply"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#GeoNear.andThen"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.unapply"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.copy"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.document"),
          mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer"),
          mmp("reactivemongo.api.commands.AggregationFramework.PipelineStageDocumentProducer"),
          mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer$"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.andThen"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.this"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.copy"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.document"),
          imt("reactivemongo.api.commands.AggregationFramework#Sort.this"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.copy"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.document"),
          mcp("reactivemongo.api.commands.CursorCommand"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#Project.document"),
          mmp("reactivemongo.api.commands.AggregationFramework#Match.document"),
          mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.copy"),
          mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.this"),
          mmp("reactivemongo.api.collections.bson.BSONQueryBuilder$"),
          mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.apply"),
          mmp("reactivemongo.api.MongoConnection.ask"),
          mmp("reactivemongo.api.MongoConnection.ask"),
          mmp("reactivemongo.api.MongoConnection.waitForPrimary"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#Aggregate.apply"),
          mtp("reactivemongo.api.commands.AggregationFramework$Aggregate"),
          mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.copy"),
          irt("reactivemongo.api.commands.AggregationFramework#Aggregate.pipeline"),
          mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.this"),
          mmp("reactivemongo.api.collections.GenericQueryBuilder.copy"),
          mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions$"),
          mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions"),
          mmp("reactivemongo.api.commands.AggregationFramework.AggregateCursorOptions"),
          mtp("reactivemongo.api.commands.AggregationFramework$Group$"),
          mtp("reactivemongo.api.collections.bson.BSONQueryBuilder$"),
          x[IncompatibleTemplateDefProblem](
            "reactivemongo.core.nodeset.Authenticating"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.compose"),
          mtp("reactivemongo.api.commands.AggregationFramework$GeoNear$"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#GeoNear.compose"),
          mcp("reactivemongo.api.commands.AggregationFramework$DocumentStage"),
          mtp("reactivemongo.api.commands.AggregationFramework$Redact"),
          mtp("reactivemongo.api.commands.AggregationFramework$GeoNear"),
          mtp("reactivemongo.api.commands.AggregationFramework$Unwind"),
          mtp("reactivemongo.api.commands.AggregationFramework$Project"),
          mtp("reactivemongo.api.commands.AggregationFramework$Out"),
          mtp("reactivemongo.api.commands.AggregationFramework$Match"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.isNotAPrimaryError"),
          mtp("reactivemongo.api.commands.AggregationFramework$Sort"),
          mtp("reactivemongo.api.commands.AggregationFramework$Group"),
          mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Redact.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Unwind.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Project.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Out.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Match.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Sort.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.name"),
          mmp("reactivemongo.api.commands.AggregationFramework#Group.apply"),
          mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
          mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.copy"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#GeoNear.document"),
          mtp("reactivemongo.core.nodeset.Authenticating$"),
          mmp("reactivemongo.api.commands.AggregationFramework#Project.apply"),
          mmp(
            "reactivemongo.api.commands.AggregationFramework#Redact.document"))
      },
      testOptions in Test += Tests.Cleanup(cl => {
        import scala.language.reflectiveCalls
        val c = cl.loadClass("Common$")
        type M = { def closeDriver(): Unit }
        val m: M = c.getField("MODULE$").get(null).asInstanceOf[M]
        m.closeDriver()
      }))).dependsOn(bsonmacros)

  lazy val bson = Project(
    s"$projectPrefix-BSON",
    file("bson"),
    settings = buildSettings).
    settings(
      libraryDependencies += specs,
      binaryIssueFilters ++= Seq(
        ProblemFilters.exclude[MissingTypesProblem](
          "reactivemongo.bson.BSONTimestamp$")))

  lazy val bsonmacros = Project(
    s"$projectPrefix-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Seq(
      libraryDependencies +=
        "org.scala-lang" % "scala-compiler" % scalaVersion.value
    )).
    settings(libraryDependencies += specs).
    dependsOn(bson)
}
