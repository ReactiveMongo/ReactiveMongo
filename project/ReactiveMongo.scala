import sbt._
import sbt.Keys._
import scala.language.postfixOps

object BuildSettings {
  val buildVersion = "0.11.0-SNAPSHOT"

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") && !path.startsWith("samples")
    }
  }

  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "org.reactivemongo",
    version := buildVersion,
    scalaVersion := "2.11.2",
    crossScalaVersions  := Seq("2.11.2", "2.10.4"),
    crossVersion := CrossVersion.binary,
    javaOptions in test ++= Seq("-Xmx512m", "-XX:MaxPermSize=512m"),
    //fork in Test := true, // Don't share executioncontext between SBT CLI/tests
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits", "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(buildVersion),
    shellPrompt := ShellPrompt.buildShellPrompt,
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter,
    Travis.travisSnapshotBranches := Seq("master")) ++
  Publish.settings // ++ Format.settings

  
}

object Publish {
  def targetRepository: Def.Initialize[Option[Resolver]] = Def.setting {
    val nexus = "https://oss.sonatype.org/"
    val snapshotsR = "snapshots" at nexus + "content/repositories/snapshots"
    val releasesR  = "releases"  at nexus + "service/local/staging/deploy/maven2"
    val resolver = if (isSnapshot.value) snapshotsR else releasesR
    Some(resolver)
  }

  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo := targetRepository.value,
    credentials := Travis.credentials.value, // TODO: Review
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

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.6"

  val iteratees = "com.typesafe.play" %% "play-iteratees" % "2.3.5"

  val specs = "org.specs2" %% "specs2-core" % "2.4.9" % "test"

  val log4jVersion = "2.0.2"
  val log4j = Seq("org.apache.logging.log4j" % "log4j-api" % log4jVersion, "org.apache.logging.log4j" % "log4j-core" % log4jVersion)

  val shapelessTest = "com.chuusai" % "shapeless" % "2.0.0" %
  Test cross CrossVersion.binaryMapped {
    case "2.10" => "2.10.4"
    case x => x
  }
}

object ReactiveMongoBuild extends Build {
  import BuildSettings._
  import Resolvers._
  import Dependencies._
  import sbtunidoc.{ Plugin => UnidocPlugin }

  val projectPrefix = "ReactiveMongo"

  lazy val reactivemongo =
    Project(
      s"$projectPrefix-Root",
      file("."),
      settings = buildSettings ++ (publishArtifact := false) ).
    settings(UnidocPlugin.unidocSettings: _*).
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
        shapelessTest,
        specs) ++ log4j,
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
    settings(libraryDependencies += Dependencies.specs)

  lazy val bsonmacros = Project(
    s"$projectPrefix-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Seq(
      libraryDependencies +=
        "org.scala-lang" % "scala-compiler" % scalaVersion.value
    )).
    settings(libraryDependencies += Dependencies.specs).
    dependsOn(bson)
}

object Travis {
  val travisSnapshotBranches =
    SettingKey[Seq[String]]("branches that can be published on sonatype")

  val credentials: Def.Initialize[Seq[sbt.Credentials]] = Def.setting {
    val isTravis = sys.env.get("TRAVIS").exists(_ == "true")
    val noCredentials = Seq.empty[sbt.Credentials]

    if (!isTravis) noCredentials else {
      import scala.util.Properties.isJavaAtLeast

      val isSnapshot = version.value.endsWith("SNAPSHOT")
      val isBranchAcceptable = sys.env.get("TRAVIS_BRANCH").
        exists(branch => travisSnapshotBranches.value.contains(branch))
      val isJavaVersion = !isJavaAtLeast("1.7")
      val isPR = sys.env.get("TRAVIS_PULL_REQUEST").exists(_ == "true")
      val user = sys.env.get("SONATYPE_USER").map(_.trim).filterNot(_.isEmpty)
      val pass = sys.env.get("SONATYPE_PASSWORD").map(_.trim).filterNot(_.isEmpty)
      @inline def creds = user.flatMap(u => pass.map(u -> _))
      @inline def debugUser = user.getOrElse("<missing-user>")
      @inline def debugPass = {
        import org.apache.commons.codec.digest.DigestUtils
        pass.fold("<missing-password>")(DigestUtils.sha1Hex)
      }

      creds.fold({
        sys.error(s"missing Sonatype credentials: SONATYPE_USER=$debugUser, SONATYPE_PASSWORD=$debugPass")
      }) {
        case (username, password) =>
          if (isSnapshot && !isPR && isBranchAcceptable) {

            println(s"publishing from travis (user = $username : SHA1($debugPass)) ...")

            Seq(Credentials(
            "Sonatype Nexus Repository Manager",
              "oss.sonatype.org",
              username, password))

        } else {
            println(s"not publishing to Sonatype: isSnapshot=$isSnapshot, isNotPR=${!isPR}, isBranchAcceptable=$isBranchAcceptable, javaVersionLessThen_1_7=$isJavaVersion")

            noCredentials
          }
      }
    }
  }
}
