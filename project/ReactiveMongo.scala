import sbt._
import sbt.Keys._

object BuildSettings {
  val buildVersion = "0.9-SNAPSHOT"

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") && !path.startsWith("samples")
    }
  }

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.reactivemongo",
    version := buildVersion,
    scalaVersion := "2.10.0",
    crossScalaVersions := Seq("2.10.0"),
    crossVersion := CrossVersion.binary,
    scalacOptions ++= Seq("-unchecked", "-deprecation" /*, "-Xlog-implicits", "-Yinfer-debug", "-Xprint:typer", "-Yinfer-debug", "-Xlog-implicits", "-Xprint:typer"*/ ),
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits"),
    shellPrompt := ShellPrompt.buildShellPrompt,
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter) ++ Publish.settings // ++ Format.settings
}

object Publish {
  object TargetRepository {
    def local: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
      val localPublishRepo = "/Volumes/Data/code/repository"
      if (version.trim.endsWith("SNAPSHOT"))
        Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
      else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
    }
    def sonatype: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (version.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  }
  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo <<= TargetRepository.local,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("http://reactivemongo.org")),
    pomExtra := (
      <scm>
        <url>git://github.com/zenexity/ReactiveMongo.git</url>
        <connection>scm:git://github.com/zenexity/ReactiveMongo.git</connection>
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
  val netty = "io.netty" % "netty" % "3.3.1.Final" cross CrossVersion.Disabled

  def akkaActor(sv: String) = sv match {
    case "2.10.0" => "com.typesafe.akka" %% "akka-actor" % "2.1.0"
    case "2.10.1" => "com.typesafe.akka" %% "akka-actor" % "2.1.0"
  }

  def iteratees(sv: String) = sv match {
    case "2.10.0" => "play" %% "play-iteratees" % "2.1.0"
    case "2.10.1" => "play" %% "play-iteratees" % "2.1.1"
  }

  val logbackVer = "1.0.9"
  val logback = Seq(
    "ch.qos.logback" % "logback-core" % logbackVer,
    "ch.qos.logback" % "logback-classic" % logbackVer)

  def specs(sv: String) = sv match {
    case "2.10.0" => "org.specs2" % "specs2" % "1.13" % "test" cross CrossVersion.binary
    case "2.10.1" => "org.specs2" % "specs2" % "1.13" % "test" cross CrossVersion.binary
  }

  val junit = "junit" % "junit" % "4.8" % "test" cross CrossVersion.Disabled
  val testDeps = Seq(junit)
}

object ReactiveMongoBuild extends Build {
  import BuildSettings._
  import Resolvers._
  import Dependencies._

  lazy val reactivemongo = Project(
    "ReactiveMongo-Root",
    file("."),
    settings = buildSettings ++ Unidoc.settings ++ Seq(
      publish := {}
    )) aggregate(driver, bson, bsonmacros)

  lazy val driver = Project(
    "ReactiveMongo",
    file("driver"),
    settings = buildSettings ++ Seq(
      resolvers := resolversList,
      libraryDependencies <++= (scalaVersion)(sv => Seq(
        netty,
        akkaActor(sv),
        iteratees(sv),
        specs(sv)) ++ logback ++ testDeps))) dependsOn (bsonmacros)

  lazy val bson = Project(
    "ReactiveMongo-BSON",
    file("bson"),
    settings = buildSettings)

  lazy val bsonmacros = Project(
    "ReactiveMongo-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Seq(
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-compiler" % _)
    )) dependsOn (bson)
}

