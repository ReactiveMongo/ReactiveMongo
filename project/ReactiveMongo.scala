
import sbt._
import sbt.Keys._

object BuildSettings {
  val buildVersion = "0.1-SNAPSHOT"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "reactivemongo",
    version := buildVersion,
    scalaVersion := "2.10.0",
    crossScalaVersions := Seq("2.10.0"),
    crossVersion := CrossVersion.binary,
    shellPrompt := ShellPrompt.buildShellPrompt
  )// ++ Format.settings

}



object Publish {
  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://github.com/zenexity/ReactiveMongo")),
    pomExtra := (
      <scm>
        <url>git://github.com/zenexity/ReactiveMongo.git</url>
        <connection>scm:git://github.com/zenexity/ReactiveMongo.git</connection>
      </scm>
        <developers>
          <developer>
            <id>sgodbillon</id>
            <name>Stephane Godbillon</name>
            <url>http://www.zenexity.com/</url>
          </developer>
        </developers>)
  )
}


object Format {

  import com.typesafe.sbt.SbtScalariform._

  lazy val settings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := formattingPreferences
  )

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, true).
      setPreference(AlignSingleLineCaseStatements, true).
      setPreference(CompactControlReadability, true).
      setPreference(CompactStringConcatenation, true).
      setPreference(DoubleIndentClassDeclaration, true).
      setPreference(FormatXml, true).
      setPreference(IndentLocalDefs, true).
      setPreference(IndentPackageBlocks, true).
      setPreference(IndentSpaces, 2).
      setPreference(MultilineScaladocCommentsStartOnFirstLine, true).
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
      getOrElse "-" stripPrefix "## "
    )

  val buildShellPrompt = {
    (state: State) => {
      val currProject = Project.extract(state).currentProject.id
      "%s:%s:%s> ".format(
        currProject, currBranch, BuildSettings.buildVersion
      )
    }
  }
}


object Resolvers {
  val typesafe = Seq(
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"
  )
  val resolversList = typesafe
}


object Dependencies {
  val netty = "io.netty" % "netty" % "3.3.1.Final" cross CrossVersion.Disabled

  def akkaActor(sv: String) = sv match {
    case "2.10.0" => "com.typesafe.akka" %% "akka-actor" % "2.1.0"
  }

  def iteratees(sv: String) = sv match {
    case "2.10.0" => "play" %% "play-iteratees" % "2.1-RC1"
  }

  val logbackVer = "1.0.9"
  val logback = Seq(
    "ch.qos.logback" % "logback-core" % logbackVer,
    "ch.qos.logback" % "logback-classic" % logbackVer
  )

//  val compileDeps = Seq(netty, akkaActor, iteratees) ++ logback


  def specs(sv: String) = sv match {
    case "2.10.0" => "org.specs2" % "specs2" % "1.13" % "test" cross CrossVersion.binary
  }

  val junit = "junit" % "junit" % "4.8" % "test" cross CrossVersion.Disabled
  val testDeps = Seq(junit)


}

object ReactiveMongoBuild extends Build {

  import BuildSettings._
  import Resolvers._
  import Dependencies._

  lazy val reactivemongo = Project(
    "reactivemongo",
    file("."),
    settings = buildSettings ++ Seq(
      resolvers := resolversList,
      libraryDependencies <++= (scalaVersion)(sv => Seq(
        netty,
        akkaActor(sv),
        iteratees(sv),
        specs(sv)
      ) ++ logback ++ testDeps
      )
    )
  )
}

