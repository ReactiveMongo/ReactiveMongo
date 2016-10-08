import sbt._
import sbt.Keys._
import scala.language.postfixOps

object BuildSettings {
  val buildVersion = "0.12-RC5"

  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") &&
        !path.startsWith("samples")
    }
  }

  val baseSettings = Seq(
    organization := "org.reactivemongo",
    version := buildVersion,
    shellPrompt := ShellPrompt.buildShellPrompt)

  val buildSettings = Defaults.coreDefaultSettings ++ baseSettings ++ Seq(
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.5", "2.11.8"),
    crossVersion := CrossVersion.binary,
    //parallelExecution in Test := false,
    //fork in Test := true, // Don't share executioncontext between SBT CLI/tests
    scalacOptions in Compile ++= Seq(
      "-unchecked", "-deprecation", "-target:jvm-1.6", "-Ywarn-unused-import"),
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation",
      /*"-diagrams", */"-implicits", "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(buildVersion),
    scalacOptions in Compile := {
      val opts = (scalacOptions in Compile).value

      if (scalaVersion.value != "2.11.8") {
        opts.filter(_ != "-Ywarn-unused-import")
      } else opts
    },
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

  val majorVersion = "0.12"
  val previousVersion = "0.11.0"

  val missingMethodInOld: ProblemFilter = {
    case mmp @ MissingMethodProblem(_) if (
      mmp.affectedVersion == ClassVersion.Old) => false

    case MissingMethodProblem(old) => !old.isAccessible
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

  val siteUrl = "http://reactivemongo.org"

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
    homepage := Some(url(siteUrl)),
    autoAPIMappings := true,
    apiURL := Some(url(s"$siteUrl/release/$majorVersion/api/")),
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
      setPreference(AlignParameters, false).
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
      setPreference(RewriteArrowSymbols, false).
      setPreference(SpaceBeforeColon, false).
      setPreference(SpaceInsideBrackets, false).
      setPreference(SpacesAroundMultiImports, true).
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
  // TODO: Update
  val akka = {
    val ver = sys.env.get("AKKA_VERSION").getOrElse("2.3.13")

    Seq(
      "com.typesafe.akka" %% "akka-actor" % ver,
      "com.typesafe.akka" %% "akka-testkit" % ver % Test)
  }

  val playIteratees = "com.typesafe.play" %% "play-iteratees" % "2.3.10"

  val specsVer = "3.8.3"
  val specs = "org.specs2" %% "specs2-core" % specsVer % Test

  val slf4jVer = "1.7.12"
  val log4jVer = "2.5"

  val slf4j = "org.slf4j" % "slf4j-api" % slf4jVer
  val slf4jSimple = "org.slf4j" % "slf4j-simple" % slf4jVer

  val logApi = Seq(
    slf4j % "provided",
    "org.apache.logging.log4j" % "log4j-api" % log4jVer // deprecated
  ) ++ Seq("log4j-core", "log4j-slf4j-impl").map(
    "org.apache.logging.log4j" % _ % log4jVer % Test)

  val shapelessTest = "com.chuusai" % "shapeless" % "2.0.0" %
  Test cross CrossVersion.binaryMapped {
    case "2.10" => "2.10.5"
    case x => x
  }

  val commonsCodec = "commons-codec" % "commons-codec" % "1.10"
}

object Findbugs {
  import scala.xml.{ NodeSeq, XML }, XML.{ loadFile => loadXML }

  import de.johoop.findbugs4sbt.{ FindBugs, ReportType }, FindBugs.{
    findbugsExcludeFilters, findbugsReportPath, findbugsReportType,
    findbugsSettings
  }

  @inline def task = FindBugs.findbugs

  val settings = findbugsSettings ++ Seq(
    findbugsReportType := Some(ReportType.PlainHtml),
    findbugsReportPath := Some(target.value / "findbugs.html"),
    findbugsExcludeFilters := {
      val commonFilters = loadXML(baseDirectory.value / ".." / "project" / (
        "findbugs-exclude-filters.xml"))

      val filters = {
        val f = baseDirectory.value / "findbugs-exclude-filters.xml"
        if (!f.exists) NodeSeq.Empty else loadXML(f).child
      }

      Some(
        <FindBugsFilter>${commonFilters.child}${filters}</FindBugsFilter>
      )
    }
  )
}

object Documentation {
  import sbtunidoc.{ Plugin => UnidocPlugin },
    UnidocPlugin.UnidocKeys._, UnidocPlugin.ScalaUnidoc

  def mappings(org: String, location: String, revision: String => String = identity)(names: String*) = Def.task[Map[File, URL]] {
    (for {
      entry: Attributed[File] <- (fullClasspath in Compile).value
      module: ModuleID <- entry.get(moduleID.key)
      if module.organization == org
      if names.exists(module.name.startsWith)
      rev = revision(module.revision)
    } yield entry.data -> url(location.format(rev))).toMap
  }

  val settings = UnidocPlugin.unidocSettings ++ Seq(
    unidocProjectFilter in (ScalaUnidoc, unidoc) := {
      inAnyProject -- inProjects(
        ReactiveMongoBuild.shaded, ReactiveMongoBuild.jmx)
    },
    apiMappings ++= mappings("org.scala-lang", "http://scala-lang.org/api/%s/")("scala-library").value
  )
}

object ReactiveMongoBuild extends Build {
  import BuildSettings._
  import Resolvers._
  import Dependencies._


  import sbtassembly.{
    AssemblyKeys, MergeStrategy, PathList, ShadeRule
  }, AssemblyKeys._
  import com.typesafe.tools.mima.core._, ProblemFilters._, Problem.ClassVersion
  import com.typesafe.tools.mima.plugin.MimaKeys.{
    binaryIssueFilters, previousArtifacts
  }

  val travisEnv = taskKey[Unit]("Print Travis CI env")

  val projectPrefix = "ReactiveMongo"

  lazy val reactivemongo =
    Project(
      s"$projectPrefix-Root",
      file("."),
      settings = buildSettings ++ Documentation.settings).
      settings(
        publishArtifact := false,
        previousArtifacts := Set.empty,
        travisEnv in Test := { // test:travisEnv from SBT CLI
          val specs = List[(String, List[String])](
            "MONGO_VER" -> List("2_6", "3"),
            "MONGO_PROFILE" -> List("default", "ssl", "rs"),
            "AKKA_VERSION" -> List("2.3.13", "2.4.8")
          )

          def integrationMatrix = specs.flatMap {
            case (key, values) => values.map(key -> _)
          }.combinations(specs.size).filterNot { flags =>
            flags.contains("MONGO_VER" -> "2_6") && flags.
              contains("MONGO_PROFILE" -> "ssl")
          }.collect {
            case flags if (flags.map(_._1).toSet.size == specs.size) =>
              "CI_CATEGORY=INTEGRATION_TESTS" :: flags.sortBy(_._1).map({
                case (k, v) => s"$k=$v"
              })
          }.map { c => s"""  - ${c mkString " "}""" }

          def matrix = (Iterator.single("  - CI_CATEGORY=UNIT_TESTS") ++ (
            integrationMatrix)).mkString("\r\n")

          println(s"Travis CI env:\r\n$matrix")
        }
      ).aggregate(bson, bsonmacros, shaded, driver, jmx)

  import scala.xml.{ Elem => XmlElem, Node => XmlNode }
  private def transformPomDependencies(tx: XmlElem => Option[XmlNode]): XmlNode => XmlNode = { node: XmlNode =>
    import scala.xml.{ NodeSeq, XML }
    import scala.xml.transform.{ RewriteRule, RuleTransformer }

    val tr = new RuleTransformer(new RewriteRule {
      override def transform(node: XmlNode): NodeSeq = node match {
        case e: XmlElem if e.label == "dependency" => tx(e) match {
          case Some(n) => n
          case _ => NodeSeq.Empty
        }

        case _ => node
      }
    })

    tr.transform(node).headOption match {
      case Some(transformed) => transformed
      case _ => sys.error("Fails to transform the POM")
    }
  }

  import de.johoop.findbugs4sbt.FindBugs.findbugsAnalyzedPath

  lazy val shaded = Project(
    s"$projectPrefix-Shaded",
    file("shaded"),
    settings = baseSettings ++ Publish.settings).
    settings(
      previousArtifacts := Set.empty,
      crossPaths := false,
      autoScalaLibrary := false,
      libraryDependencies ++= Seq(
        "io.netty" % "netty" % "3.10.5.Final" cross CrossVersion.Disabled,
        "com.google.guava" % "guava" % "19.0" cross CrossVersion.Disabled
      ),
      assemblyShadeRules in assembly := Seq(
        ShadeRule.rename("org.jboss.netty.**" -> "shaded.netty.@1").inAll,
        ShadeRule.rename("com.google.**" -> "shaded.google.@1").inAll
      ),
      pomPostProcess := transformPomDependencies { _ => None },
      makePom <<= makePom.dependsOn(assembly),
      packageBin in Compile := target.value / (
        assemblyJarName in assembly).value
    )

  private val driverFilter: Seq[(File, String)] => Seq[(File, String)] = {
    (_: Seq[(File, String)]).filter {
      case (file, name) =>
        !(name endsWith "external/reactivemongo/StaticListenerBinder.class")
    }
  } andThen BuildSettings.filter

  private val commonCleanup: ClassLoader => Unit = { cl =>
    import scala.language.reflectiveCalls

    val c = cl.loadClass("Common$")
    type M = { def close(): Unit }
    val m: M = c.getField("MODULE$").get(null).asInstanceOf[M]

    m.close()
  }

  lazy val bson = Project(
    s"$projectPrefix-BSON",
    file("bson"),
    settings = buildSettings ++ Findbugs.settings).
    settings(
      libraryDependencies ++= Seq(specs,
        "org.specs2" %% "specs2-scalacheck" % specsVer % Test,
        "org.typelevel" %% "discipline" % "0.6" % Test,
        "org.spire-math" %% "spire-laws" % "0.12.0" % Test),
      binaryIssueFilters ++= Seq(
        ProblemFilters.exclude[MissingTypesProblem](
          "reactivemongo.bson.BSONTimestamp$")))

  lazy val bsonmacros = Project(
    s"$projectPrefix-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(specs,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided")
    )).dependsOn(bson)

  val driverCleanup = taskKey[Unit]("Driver compilation cleanup")

  lazy val driver = Project(
    projectPrefix,
    file("driver"),
    settings = buildSettings ++ Findbugs.settings ++ Seq(
      resolvers := resolversList,
      compile in Compile <<= (compile in Compile).dependsOn(assembly in shaded),
      sourceGenerators in Compile <+= (version).zip(sourceManaged in Compile).map {
        case (ver, dir) =>
        val outdir = dir / "reactivemongo" / "api"
        val f = outdir / "Version.scala"

        outdir.mkdirs()

        Seq(IO.writer[File](f, "", IO.defaultCharset, false) { w =>
          w.append(s"""package reactivemongo.api
object Version {
  /** The ReactiveMongo API version */
  override val toString = "$ver"

  /** The major version (e.g. 0.12 for the release 0.12.0) */
  val majorVersion = "${Publish.majorVersion}"
}""")

          f
        })
      },
      driverCleanup := {
        val classDir = (classDirectory in Compile).value
        val extDir = {
          val d = target.value / "external" / "reactivemongo"
          d.mkdirs(); d
        }

        val classFile = "StaticListenerBinder.class"
        val listenerClass = classDir / "external" / "reactivemongo" / classFile

        streams.value.log(s"Cleanup $listenerClass ...")

        IO.move(listenerClass, extDir / classFile)
      },
      driverCleanup <<= driverCleanup.triggeredBy(compile in Compile),
      unmanagedJars in Compile := {
        val shadedDir = (target in shaded).value
        val shadedJar = (assemblyJarName in (shaded, assembly)).value

        (shadedDir / "classes").mkdirs() // Findbugs workaround

        Seq(Attributed(shadedDir / shadedJar)(AttributeMap.empty))
      },
      libraryDependencies ++= akka ++ Seq(
        playIteratees, commonsCodec, shapelessTest, specs) ++ logApi,
      findbugsAnalyzedPath += target.value / "external",
      binaryIssueFilters ++= {
        import ProblemFilters.{ exclude => x }
        @inline def mmp(s: String) = x[MissingMethodProblem](s)
        @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
        @inline def fmp(s: String) = x[FinalMethodProblem](s)
        @inline def fcp(s: String) = x[FinalClassProblem](s)
        @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
        @inline def mtp(s: String) = x[MissingTypesProblem](s)
        @inline def mcp(s: String) = x[MissingClassProblem](s)

        Seq(
          fcp("reactivemongo.api.MongoDriver$SupervisorActor"), // private
          mcp("reactivemongo.api.MongoDriver$SupervisorActor$"), // private
          fcp("reactivemongo.api.collections.bson.BSONCollection"),
          imt("reactivemongo.core.actors.AwaitingResponse.apply"), // private
          imt("reactivemongo.core.actors.AwaitingResponse.this"), // private
          mmp("reactivemongo.core.protocol.MongoHandler.this"), // private
          fcp("reactivemongo.core.nodeset.ChannelFactory"),
          mcp("reactivemongo.core.actors.RefreshAllNodes"),
          mcp("reactivemongo.core.actors.RefreshAllNodes$"),
          mmp("reactivemongo.core.actors.MongoDBSystem.DefaultConnectionRetryInterval"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.apply"),
          irt("reactivemongo.core.netty.ChannelBufferReadableBuffer.buffer"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.this"),
          irt("reactivemongo.core.netty.ChannelBufferWritableBuffer.buffer"),
          imt(
            "reactivemongo.core.netty.ChannelBufferWritableBuffer.writeBytes"),
          imt("reactivemongo.core.netty.ChannelBufferWritableBuffer.this"),
          irt("reactivemongo.core.netty.BufferSequence.merged"),
          imt("reactivemongo.core.netty.BufferSequence.this"),
          imt("reactivemongo.core.netty.BufferSequence.apply"),
          imt("reactivemongo.core.protocol.package.RichBuffer"),
          imt(
            "reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer2"),
          imt(
            "reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer4"),
          imt(
            "reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer3"),
          mtp("reactivemongo.core.protocol.MongoHandler"),
          imt("reactivemongo.core.protocol.MongoHandler.exceptionCaught"),
          imt("reactivemongo.core.protocol.MongoHandler.channelConnected"),
          imt("reactivemongo.core.protocol.MongoHandler.writeComplete"),
          imt("reactivemongo.core.protocol.MongoHandler.log"),
          imt("reactivemongo.core.protocol.MongoHandler.writeRequested"),
          imt("reactivemongo.core.protocol.MongoHandler.messageReceived"),
          imt("reactivemongo.core.protocol.MongoHandler.channelClosed"),
          imt("reactivemongo.core.protocol.MongoHandler.channelDisconnected"),
          mtp("reactivemongo.core.protocol.ResponseDecoder"),
          imt("reactivemongo.core.protocol.ResponseDecoder.decode"),
          mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
          imt("reactivemongo.core.protocol.ResponseFrameDecoder.decode"),
          imt("reactivemongo.core.protocol.BufferAccessors#BufferInteroperable.apply"),
          mtp("reactivemongo.core.protocol.RequestEncoder"),
          imt("reactivemongo.core.protocol.RequestEncoder.encode"),
          mmp("reactivemongo.core.protocol.ChannelBufferReadable.apply"),
          imt("reactivemongo.core.protocol.ChannelBufferReadable.readFrom"),
          imt("reactivemongo.core.protocol.MessageHeader.readFrom"),
          imt("reactivemongo.core.protocol.MessageHeader.apply"),
          imt("reactivemongo.core.protocol.Response.copy"),
          irt("reactivemongo.core.protocol.Response.documents"),
          imt("reactivemongo.core.protocol.Response.this"),
          imt("reactivemongo.core.protocol.ReplyDocumentIterator.apply"),
          imt("reactivemongo.core.protocol.Response.apply"),
          irt("reactivemongo.core.protocol.package#RichBuffer.writeString"),
          irt("reactivemongo.core.protocol.package#RichBuffer.buffer"),
          irt("reactivemongo.core.protocol.package#RichBuffer.writeCString"),
          imt("reactivemongo.core.protocol.package#RichBuffer.this"),
          imt("reactivemongo.core.protocol.Reply.readFrom"),
          imt("reactivemongo.core.protocol.Reply.apply"),
          imt("reactivemongo.core.nodeset.Connection.apply"),
          imt("reactivemongo.core.nodeset.Connection.copy"),
          irt("reactivemongo.core.nodeset.Connection.send"),
          irt("reactivemongo.core.nodeset.Connection.channel"),
          imt("reactivemongo.core.nodeset.Connection.this"),
          irt("reactivemongo.core.nodeset.ChannelFactory.channelFactory"),
          irt("reactivemongo.core.nodeset.ChannelFactory.create"),
          mcp("reactivemongo.api.MongoDriver$CloseWithTimeout"),
          mcp("reactivemongo.api.MongoDriver$CloseWithTimeout$"),
          mtp("reactivemongo.api.FailoverStrategy$"),
          irt(
            "reactivemongo.api.collections.GenericCollection#BulkMaker.result"),
          irt("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand.result"),
          x[IncompatibleTemplateDefProblem](
            "reactivemongo.core.actors.MongoDBSystem"),
          mcp("reactivemongo.core.actors.RequestIds"),
          mcp("reactivemongo.core.actors.RefreshAllNodes"),
          mcp("reactivemongo.core.actors.RefreshAllNodes$"),
          mmp("reactivemongo.core.actors.MongoDBSystem.DefaultConnectionRetryInterval"),
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
          mmp("reactivemongo.bson.BSONTimestamp.toString"),
          irt("reactivemongo.api.commands.Upserted._id"),
          imt("reactivemongo.api.commands.Upserted.this"),
          imt("reactivemongo.api.commands.Upserted.copy"),
          irt("reactivemongo.api.Cursor.logger"),
          mcp("reactivemongo.core.netty.package"),
          mcp("reactivemongo.core.netty.package$"),
          mcp("reactivemongo.core.netty.package$BSONDocumentNettyWritable"),
          mcp("reactivemongo.core.netty.package$BSONDocumentNettyWritable$"),
          mcp("reactivemongo.core.netty.package$BSONDocumentNettyReadable"),
          mcp("reactivemongo.core.netty.package$BSONDocumentNettyReadable$"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.apply"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.buffer"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.this"),
          imt("reactivemongo.core.netty.ChannelBufferWritableBuffer.buffer"),
          imt(
            "reactivemongo.core.netty.ChannelBufferWritableBuffer.writeBytes"),
          imt("reactivemongo.core.netty.ChannelBufferWritableBuffer.this"),
          imt("reactivemongo.core.netty.BufferSequence.merged"),
          imt("reactivemongo.core.netty.BufferSequence.this"),
          imt("reactivemongo.core.protocol.package.RichBuffer"),
          imt("reactivemongo.core.netty.ChannelBufferReadableBuffer.buffer"),
          imt("reactivemongo.core.netty.ChannelBufferWritableBuffer.buffer"),
          imt("reactivemongo.core.netty.BufferSequence.merged"),
          irt("reactivemongo.core.netty.ChannelBufferReadableBuffer.buffer"),
          irt("reactivemongo.core.netty.ChannelBufferWritableBuffer.buffer"),
          irt("reactivemongo.core.netty.BufferSequence.merged"),
          imt("reactivemongo.core.netty.BufferSequence.apply"),
          imt("reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer2"),
          imt("reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer4"),
          imt("reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer3"),
          mtp("reactivemongo.core.protocol.MongoHandler"),
          imt("reactivemongo.core.protocol.MongoHandler.exceptionCaught"),
          imt("reactivemongo.core.protocol.MongoHandler.channelConnected"),
          imt("reactivemongo.core.protocol.MongoHandler.writeComplete"),
          imt("reactivemongo.core.protocol.MongoHandler.log"),
          imt("reactivemongo.core.protocol.MongoHandler.writeRequested"),
          imt("reactivemongo.core.protocol.MongoHandler.messageReceived"),
          imt("reactivemongo.core.protocol.MongoHandler.channelClosed"),
          imt("reactivemongo.core.protocol.MongoHandler.channelDisconnected"),
          mtp("reactivemongo.core.protocol.ResponseDecoder"),
          imt("reactivemongo.core.protocol.ResponseDecoder.decode"),
          mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
          imt("reactivemongo.core.protocol.ResponseFrameDecoder.decode"),
          imt("reactivemongo.core.protocol.BufferAccessors#BufferInteroperable.apply"),
          mtp("reactivemongo.core.protocol.RequestEncoder"),
          imt("reactivemongo.core.protocol.RequestEncoder.encode"),
          mmp("reactivemongo.core.protocol.ChannelBufferReadable.apply"),
          imt("reactivemongo.core.protocol.ChannelBufferReadable.readFrom"),
          imt("reactivemongo.core.protocol.MessageHeader.readFrom"),
          imt("reactivemongo.core.protocol.MessageHeader.apply"),
          imt("reactivemongo.core.protocol.Response.copy"),
          irt("reactivemongo.core.protocol.Response.documents"),
          imt("reactivemongo.core.protocol.Response.this"),
          imt("reactivemongo.core.protocol.ReplyDocumentIterator.apply"),
          imt("reactivemongo.core.protocol.Response.apply"),
          irt("reactivemongo.core.protocol.package#RichBuffer.writeString"),
          irt("reactivemongo.core.protocol.package#RichBuffer.buffer"),
          irt("reactivemongo.core.protocol.package#RichBuffer.writeCString"),
          imt("reactivemongo.core.protocol.package#RichBuffer.this"),
          imt("reactivemongo.core.protocol.Reply.readFrom"),
          imt("reactivemongo.core.protocol.Reply.apply"),
          imt("reactivemongo.core.nodeset.Connection.apply"),
          imt("reactivemongo.core.nodeset.Connection.copy"),
          irt("reactivemongo.core.nodeset.Connection.send"),
          irt("reactivemongo.core.nodeset.Connection.channel"),
          imt("reactivemongo.core.nodeset.Connection.this"),
          ProblemFilters.exclude[FinalClassProblem](
            "reactivemongo.core.nodeset.ChannelFactory"),
          irt("reactivemongo.core.nodeset.ChannelFactory.channelFactory"),
          irt("reactivemongo.core.nodeset.ChannelFactory.create"),
          mcp("reactivemongo.api.MongoDriver$CloseWithTimeout"),
          mcp("reactivemongo.api.MongoDriver$CloseWithTimeout$"),
          mtp("reactivemongo.api.FailoverStrategy$"),
          irt("reactivemongo.api.collections.GenericCollection#BulkMaker.result"),
          irt("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand.result"),
          imt("reactivemongo.core.protocol.Response.documents"),
          imt("reactivemongo.core.protocol.package#RichBuffer.writeString"),
          imt("reactivemongo.core.protocol.package#RichBuffer.buffer"),
          imt("reactivemongo.core.protocol.package#RichBuffer.writeCString"),
          imt("reactivemongo.core.nodeset.Connection.send"),
          imt("reactivemongo.core.nodeset.Connection.channel"),
          imt("reactivemongo.core.nodeset.ChannelFactory.channelFactory"),
          imt("reactivemongo.core.nodeset.ChannelFactory.create"),
          imt("reactivemongo.api.collections.GenericCollection#BulkMaker.result"),
          imt("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand.result"),
          mcp("reactivemongo.api.MongoConnection$IsKilled"),
          mcp("reactivemongo.api.MongoConnection$IsKilled$"),
          mcp("reactivemongo.api.commands.tst2"),
          mcp("reactivemongo.api.commands.tst2$"),
          mcp("reactivemongo.api.collections.GenericCollection$Mongo24BulkInsert"),
          mtp("reactivemongo.api.commands.DefaultWriteResult"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.fillInStackTrace"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.isUnauthorized"),
          mmp("reactivemongo.api.commands.DefaultWriteResult.getMessage"),
          irt("reactivemongo.api.commands.DefaultWriteResult.originalDocument"),
          irt("reactivemongo.core.commands.Getnonce.ResultMaker"),
          irt("reactivemongo.core.protocol.RequestEncoder.logger"),
          irt("reactivemongo.api.MongoConnection.killed"),
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
            "reactivemongo.api.commands.AggregationFramework#Redact.document"),
          mmp("reactivemongo.api.DefaultDB.sister"),
          mmp("reactivemongo.api.DB.sister"),
          mtp("reactivemongo.api.MongoDriver$AddConnection$"),
          mmp("reactivemongo.api.MongoDriver#AddConnection.apply"),
          mmp("reactivemongo.api.MongoDriver#AddConnection.copy"),
          mmp("reactivemongo.api.MongoDriver#AddConnection.this"),
          mmp("reactivemongo.api.indexes.Index.copy"),
          mmp("reactivemongo.api.indexes.Index.this"),
          mtp("reactivemongo.api.indexes.Index$"),
          mmp("reactivemongo.api.indexes.Index.apply"))
      },
      testOptions in Test += Tests.Cleanup(commonCleanup),
      mappings in (Compile, packageBin) ~= driverFilter,
      //mappings in (Compile, packageDoc) ~= driverFilter,
      mappings in (Compile, packageSrc) ~= driverFilter,
      apiMappings ++= Documentation.mappings("com.typesafe.akka", "http://doc.akka.io/api/akka/%s/")("akka-actor").value ++ Documentation.mappings("com.typesafe.play", "http://playframework.com/documentation/%s/api/scala/index.html", _.replaceAll("[\\d]$", "x"))("play-iteratees").value
    )).dependsOn(bsonmacros, shaded)

  private val providedInternalDeps: XmlNode => XmlNode = {
    import scala.xml.NodeSeq
    import scala.xml.transform.{ RewriteRule, RuleTransformer }

    val asProvided = new RuleTransformer(new RewriteRule {
      override def transform(node: XmlNode): NodeSeq = node match {
        case e: XmlElem if e.label == "scope" =>
          NodeSeq.Empty

        case _ => node
      }
    })

    transformPomDependencies { dep: scala.xml.Elem =>
      if ((dep \ "groupId").text == "org.reactivemongo") {
        asProvided.transform(dep).headOption.collectFirst {
          case e: XmlElem => e.copy(
            child = e.child :+ <scope>provided</scope>)
        }
      } else Some(dep)
    }
  }

  lazy val jmx = Project(
    s"$projectPrefix-JMX",
    file("jmx"),
    settings = buildSettings ++ Findbugs.settings).
    settings(
      previousArtifacts := Set.empty,
      testOptions in Test += Tests.Cleanup(commonCleanup),
      libraryDependencies ++= Seq(specs) ++ logApi,
      pomPostProcess := providedInternalDeps
    ).dependsOn(driver)
}
