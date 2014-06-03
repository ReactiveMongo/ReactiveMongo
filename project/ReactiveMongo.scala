import sbt._
import sbt.Keys._

object BuildSettings {
  val buildVersion = "0.11.0-SNAPSHOT"

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") && !path.startsWith("samples")
    }
  }

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.reactivemongo",
    version := buildVersion,
    scalaVersion := "2.11.0",
    crossScalaVersions  := Seq("2.11.0", "2.10.4"),
    crossVersion := CrossVersion.binary,
    javaOptions in test ++= Seq("-Xmx512m", "-XX:MaxPermSize=512m"),
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits", "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(buildVersion),
    shellPrompt := ShellPrompt.buildShellPrompt,
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter) ++ Publish.settings // ++ Format.settings
}

object Publish {
  def targetRepository: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (version.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }

  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo <<= targetRepository,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
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
  val netty = "io.netty" % "netty" % "3.6.5.Final" cross CrossVersion.Disabled

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.2"

  val iteratees = "com.typesafe.play" %% "play-iteratees" % "2.3.0-RC1"

  val specs = "org.specs2" %% "specs2" % "2.3.12" % "test"
  val scalatest = "org.scalatest" % "scalatest_2.10" % "2.1.7" % "test"

  val log4jVersion = "2.0-beta9"
  val log4j = Seq("org.apache.logging.log4j" % "log4j-api" % log4jVersion, "org.apache.logging.log4j" % "log4j-core" % log4jVersion)
}

object ReactiveMongoBuild extends Build {
  import BuildSettings._
  import Resolvers._
  import Dependencies._
  import sbtunidoc.{ Plugin => UnidocPlugin }

  lazy val reactivemongo =
    Project(
      "ReactiveMongo-Root",
      file("."),
      settings = buildSettings ++ (publishArtifact := false) ).
    settings(UnidocPlugin.unidocSettings: _*).
    aggregate(driver, bson, bsonmacros)

  lazy val driver = Project(
    "ReactiveMongo",
    file("driver"),
    settings = buildSettings ++ Seq(
      resolvers := resolversList,
      libraryDependencies <++= (scalaVersion)(sv => Seq(
        netty,
        akkaActor,
        iteratees,
        specs) ++ log4j))) dependsOn (bsonmacros)

  lazy val bson = Project(
    "ReactiveMongo-BSON",
    file("bson"),
    settings = buildSettings).
    settings(libraryDependencies += Dependencies.specs)

  lazy val bsonmacros = Project(
    "ReactiveMongo-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Seq(
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-compiler" % _)
    )).
    settings(libraryDependencies += Dependencies.specs).
    dependsOn (bson)

    lazy val bsonmacrosspec = Project(
    "ReactiveMongo-BSON-Macros-Spec",
    file("macros-spec"),
    settings = buildSettings ++ Seq(
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-compiler" % _)
    )).
    settings(libraryDependencies += Dependencies.specs).
	settings(libraryDependencies += Dependencies.scalatest).
    dependsOn (bson).
    dependsOn (bsonmacros)
}

