import sbt._
import sbt.Keys._
import scala.language.postfixOps

object BuildSettings {
  val filter = { (ms: Seq[(File, String)]) =>
    ms filter {
      case (file, path) =>
        path != "logback.xml" && !path.startsWith("toignore") &&
        !path.startsWith("samples")
    }
  }

  val baseSettings = Seq(organization := "org.reactivemongo")

  val buildSettings = Defaults.coreDefaultSettings ++ baseSettings ++ Seq(
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.5", "2.11.8", "2.12.1"),
    crossVersion := CrossVersion.binary,
    //parallelExecution in Test := false,
    //fork in Test := true, // Don't share executioncontext between SBT CLI/tests
    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-numeric-widen",
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",
      "-g:vars"
    ),
    scalacOptions in Compile ++= {
      if (!scalaVersion.value.startsWith("2.11.")) Nil
      else Seq(
        "-Yconst-opt",
        "-Yclosure-elim",
        "-Ydead-code",
        "-Yopt:_"
      )
    },
    scalacOptions in Compile ++= {
      if (scalaVersion.value startsWith "2.10.") Nil
      else Seq(
        "-Ywarn-infer-any",
        "-Ywarn-unused",
        "-Ywarn-unused-import",
        "-Xlint:missing-interpolator"
      )
    },
    scalacOptions in Compile ++= {
      if (!scalaVersion.value.startsWith("2.12.")) Seq("-target:jvm-1.6")
      else Nil
    },
    scalacOptions in (Compile, console) ~= {
      _.filterNot { opt => opt.startsWith("-X") || opt.startsWith("-Y") }
    },
    scalacOptions in (Test, console) ~= {
      _.filterNot { opt => opt.startsWith("-X") || opt.startsWith("-Y") }
    },
    scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation",
      /*"-diagrams", */"-implicits", "-skip-packages", "samples"),
    scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API"),
    scalacOptions in (Compile, doc) ++= Opts.doc.version(Release.major.value),
    scalacOptions in Compile := {
      val opts = (scalacOptions in Compile).value

      if (scalaVersion.value != "2.10.5") opts
      else {
        opts.filter(_ != "-Ywarn-unused-import")
      }
    },
    mappings in (Compile, packageBin) ~= filter,
    mappings in (Compile, packageSrc) ~= filter,
    mappings in (Compile, packageDoc) ~= filter
  ) ++ Publish.settings ++ Format.settings ++ (
    Release.settings ++ Publish.mimaSettings)
}

object Resolvers {
  val typesafe = Seq(
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/")
  val resolversList = typesafe
}

object Dependencies {
  val akka = Def.setting[Seq[ModuleID]] {
    val ver = sys.env.get("AKKA_VERSION").getOrElse {
      if (scalaVersion.value startsWith "2.12.") "2.4.14"
      else "2.3.13"
    }

    Seq(
      "com.typesafe.akka" %% "akka-actor" % ver,
      "com.typesafe.akka" %% "akka-testkit" % ver % Test)
  }

  val playIteratees = Def.setting[ModuleID] {
    val ver = sys.env.get("ITERATEES_VERSION").getOrElse {
      if (scalaVersion.value startsWith "2.10.") "2.3.9"
      else "2.6.1"
    }

    "com.typesafe.play" %% "play-iteratees" % ver
  }

  val specsVer = "3.8.6"
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

  val shapelessTest = "com.chuusai" %% "shapeless" % "2.3.2"

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

  import de.johoop.cpd4sbt.CopyPasteDetector

  val travisEnv = taskKey[Unit]("Print Travis CI env")

  val projectPrefix = "ReactiveMongo"

  lazy val reactivemongo = Project(
    s"$projectPrefix-Root",
    file("."),
    settings = buildSettings ++ Documentation.settings).
    settings(
      publishArtifact := false,
      previousArtifacts := Set.empty,
      travisEnv in Test := { // test:travisEnv from SBT CLI
        val (akkaLower, akkaUpper) = "2.3.13" -> "2.5.1"
        val (playLower, playUpper) = "2.3.8" -> "2.6.1"
        val (mongoLower, mongoUpper) = "2_6" -> "3_4"

        val specs = List[(String, List[String])](
          "MONGO_VER" -> List("2_6", "3", "3_4"),
          "MONGO_PROFILE" -> List(
            "default", "invalid-ssl", "mutual-ssl", "rs"),
          "AKKA_VERSION" -> List(akkaLower, akkaUpper),
          "ITERATEES_VERSION" -> List(playLower, playUpper)
        )

        lazy val integrationEnv = specs.flatMap {
          case (key, values) => values.map(key -> _)
        }.combinations(specs.size).filterNot { flags =>
          /* chrono-compat exclusions */
          (flags.contains("AKKA_VERSION" -> akkaLower) && flags.
            contains("ITERATEES_VERSION" -> playUpper)) ||
          (flags.contains("AKKA_VERSION" -> akkaUpper) && flags.
            contains("ITERATEES_VERSION" -> playLower)) ||
          (flags.contains("MONGO_VER" -> mongoLower) && flags.
            contains("ITERATEES_VERSION" -> playUpper)) ||
          (flags.contains("MONGO_VER" -> mongoUpper) && flags.
            contains("ITERATEES_VERSION" -> playLower)) ||
          (flags.contains("MONGO_VER" -> mongoUpper) && flags.
            contains("AKKA_VERSION" -> akkaLower)) ||
          (flags.contains("AKKA_VERSION" -> akkaLower) && flags.
            contains("MONGO_PROFILE" -> "rs")) ||
          /* profile exclusions */
          (!flags.contains("MONGO_VER" -> mongoUpper) && flags.
            contains("MONGO_PROFILE" -> "invalid-ssl")) ||
          (!flags.contains("MONGO_VER" -> mongoUpper) && flags.
            contains("MONGO_PROFILE" -> "mutual-ssl")) ||
          (flags.contains("MONGO_VER" -> mongoLower) && flags.
            contains("MONGO_PROFILE" -> "rs") && flags.
            contains("ITERATEES_VERSION" -> playLower))
        }.collect {
          case flags if (flags.map(_._1).toSet.size == specs.size) =>
            ("CI_CATEGORY" -> "INTEGRATION_TESTS") :: flags.sortBy(_._1)
        }.toList

        @inline def integrationVars(flags: List[(String, String)]): String =
          flags.map { case (k, v) => s"$k=$v" }.mkString(" ")

        def integrationMatrix =
          integrationEnv.map(integrationVars).map { c => s"  - $c" }

        def matrix = (List("env:", "  - CI_CATEGORY=UNIT_TESTS").iterator ++ (
          integrationMatrix :+ "matrix: " :+ "  exclude: ") ++ List(
          "    - scala: 2.10.5",
            "      jdk: oraclejdk8",
            "      env: CI_CATEGORY=UNIT_TESTS") ++ List(
          "    - scala: 2.11.8",
              "      jdk: oraclejdk7",
              "      env: CI_CATEGORY=UNIT_TESTS") ++ List(
          "    - scala: 2.12.1",
                "      jdk: oraclejdk7",
                "      env: CI_CATEGORY=UNIT_TESTS") ++ (
          integrationEnv.flatMap { flags =>
            if (flags.contains("CI_CATEGORY" -> "INTEGRATION_TESTS") &&
              (/* time-compat exclusions: */
                flags.contains("ITERATEES_VERSION" -> playUpper) ||
                  flags.contains("AKKA_VERSION" -> akkaUpper) ||
                  flags.contains("MONGO_VER" -> mongoUpper) ||
                  /* profile priority exclusions: */
                  flags.contains("MONGO_PROFILE" -> "invalid-ssl") ||
                  flags.contains("MONGO_PROFILE" -> "mutual-ssl"))) {
              List(
                "    - scala: 2.10.5",
                s"      env: ${integrationVars(flags)}",
                "    - jdk: oraclejdk7",
                s"      env: ${integrationVars(flags)}"
              )
            } else if (flags.contains("CI_CATEGORY" -> "INTEGRATION_TESTS") &&
              (/* time-compat exclusions: */
                flags.contains("ITERATEES_VERSION" -> playLower) ||
                  flags.contains("AKKA_VERSION" -> akkaLower) ||
                  flags.contains("MONGO_VER" -> mongoLower)
              )) {
              List(
                "    - scala: 2.12.1",
                s"      env: ${integrationVars(flags)}",
                "    - jdk: oraclejdk8",
                s"      env: ${integrationVars(flags)}"
              )
            } else List.empty[String]
          })
        ).mkString("\r\n")

        println(s"# Travis CI env\r\n$matrix")
      }
    ).aggregate(bson, bsonmacros, shaded, driver, jmx).
    enablePlugins(CopyPasteDetector)

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
    settings = baseSettings ++ Publish.settings ++ Seq(
      previousArtifacts := Set.empty,
      crossPaths := false,
      autoScalaLibrary := false,
      libraryDependencies ++= Seq(
        "io.netty" % "netty" % "3.10.6.Final" cross CrossVersion.Disabled,
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
    settings = buildSettings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(specs,
        "org.specs2" %% "specs2-scalacheck" % specsVer % Test,
        "org.typelevel" %% "discipline" % "0.7.2" % Test,
        "org.spire-math" %% "spire-laws" % "0.13.0" % Test),
      binaryIssueFilters ++= {
        import ProblemFilters.{ exclude => x }
        @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)

        Seq(
          x[MissingTypesProblem]("reactivemongo.bson.BSONTimestamp$"),
          irt("reactivemongo.bson.Producer.noneOptionValue2Producer"),
          irt("reactivemongo.bson.Producer.noneOptionValueProducer"),
          irt("reactivemongo.bson.Producer.nameOptionValue2Producer"),
          irt("reactivemongo.bson.Producer.valueProducer"),
          irt("reactivemongo.bson.Producer.optionValueProducer")
        )
      }
    )
  ).enablePlugins(CopyPasteDetector)

  lazy val bsonmacros = Project(
    s"$projectPrefix-BSON-Macros",
    file("macros"),
    settings = buildSettings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(specs,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided")
    ))
    .enablePlugins(CopyPasteDetector)
    .dependsOn(bson)

  val driverCleanup = taskKey[Unit]("Driver compilation cleanup")

  lazy val driver = Project(
    projectPrefix,
    file("driver"),
    settings = buildSettings ++ Findbugs.settings ++ Seq(
      resolvers := resolversList,
      compile in Compile <<= (compile in Compile).dependsOn(assembly in shaded),
      sourceGenerators in Compile += Def.task {
        val ver = version.value
        val dir = (sourceManaged in Compile).value
        val outdir = dir / "reactivemongo" / "api"
        val f = outdir / "Version.scala"

        outdir.mkdirs()

        Seq(IO.writer[File](f, "", IO.defaultCharset, false) { w =>
          w.append(s"""package reactivemongo.api
object Version {
  /** The ReactiveMongo API version */
  override val toString = "$ver"

  /** The major version (e.g. 0.12 for the release 0.12.0) */
  val majorVersion = "${Release.major.value}"
}""")

          f
        })
      }.taskValue,
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
      libraryDependencies ++= akka.value ++ Seq(
        playIteratees.value, commonsCodec, shapelessTest, specs) ++ logApi,
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
          mcp("reactivemongo.api.MongoConnection$MonitorActor$"),
          mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper$"), // priv
          mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper"), // priv
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
    )
  ).enablePlugins(CopyPasteDetector).
    dependsOn(bsonmacros, shaded)

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
    ).enablePlugins(CopyPasteDetector).
    dependsOn(driver)
}
