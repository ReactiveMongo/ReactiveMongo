import scala.xml.{
  Attribute => XmlAttr,
  Elem => XmlElem,
  Node => XmlNode,
  NodeSeq,
  XML
}
import scala.xml.transform.{ RewriteRule, RuleTransformer }

import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaBinaryIssueFilters

import com.github.sbt.cpd.CpdPlugin

import sbtassembly.AssemblyKeys, AssemblyKeys._

import com.github.sbt.findbugs.FindbugsKeys.findbugsAnalyzedPath

final class Driver(
  bsonmacros: Project,
  shaded: Project,
  linuxShaded: Project,
  osxShaded: Project) {

  import Dependencies._

  lazy val module = Project("ReactiveMongo", file("driver")).
    enablePlugins(CpdPlugin).
    dependsOn(bsonmacros).
    settings(
      Common.settings ++ Findbugs.settings ++ Seq(
        resolvers := Resolvers.resolversList,
        compile in Compile := (compile in Compile).
          dependsOn(assembly in shaded).value,
        sourceGenerators in Compile += Def.task {
          val ver = version.value
          val dir = (sourceManaged in Compile).value
          val outdir = dir / "reactivemongo" / "api"
          val f = outdir / "Version.scala"
          val major = Release.major.value

          outdir.mkdirs()

          Seq(IO.writer[File](f, "", IO.defaultCharset, false) { w =>
            w.append(s"""package reactivemongo.api
object Version {
  /** The ReactiveMongo API version */
  override val toString = "$ver"

  /** The major version (e.g. 0.12 for the release 0.12.0) */
  val majorVersion = "${major}"
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
        driverCleanup := driverCleanup.triggeredBy(compile in Compile).value,
        unmanagedJars in Compile := {
          val dir = (target in shaded).value
          val jar = (assemblyJarName in (shaded, assembly)).value

          (dir / "classes").mkdirs() // Findbugs workaround

          Seq(Attributed(dir / jar)(AttributeMap.empty))
        },
        libraryDependencies ++= {
          if (!scalaVersion.value.startsWith("2.13.")) {
            Seq(playIteratees.value)
          } else {
            Seq.empty
          }
        },
        libraryDependencies ++= akka.value ++ Seq(
            "dnsjava" % "dnsjava" % "2.1.9",
          commonsCodec,
            shapelessTest % Test, specs.value) ++ logApi,
        findbugsAnalyzedPath += target.value / "external",
        mimaBinaryIssueFilters ++= {
          import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

          @inline def mmp(s: String) = x[MissingMethodProblem](s)
          @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
          @inline def fmp(s: String) = x[FinalMethodProblem](s)
          @inline def fcp(s: String) = x[FinalClassProblem](s)
          @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
          @inline def mtp(s: String) = x[MissingTypesProblem](s)
          @inline def mcp(s: String) = x[MissingClassProblem](s)

          // 2.12
          val f212: ProblemFilter = {
            case MissingClassProblem(cls) => {
              !(cls.fullName.startsWith(
                "reactivemongo.api.commands.AggregationFramework") ||
                cls.fullName.startsWith(
                  "reactivemongo.api.commands.GroupAggregation"))
            }

            case MissingTypesProblem(cls, _) => {
              !(cls.fullName.startsWith(
                "reactivemongo.api.commands.AggregationFramework") ||
                cls.fullName.startsWith(
                  "reactivemongo.api.commands.GroupAggregation"))
            }

            case _ => true
          }

          Seq( // TODO: according Scala version
            f212,
            fcp("reactivemongo.api.collections.bson.BSONCollection"),
            fcp("reactivemongo.core.nodeset.ChannelFactory"),
            fcp("reactivemongo.core.protocol.ResponseInfo"),
            fmp("reactivemongo.api.collections.bson.BSONCollection.fullCollectionName"),
            fmp("reactivemongo.api.commands.AggregationFramework#GeoNear.toString"),
            fmp("reactivemongo.api.commands.AggregationFramework#Group.toString"),
            fmp("reactivemongo.api.commands.AggregationFramework#Match.toString"),
            fmp("reactivemongo.api.commands.AggregationFramework#Project.toString"),
            fmp("reactivemongo.api.commands.AggregationFramework#Redact.toString"),
            fmp("reactivemongo.api.commands.AggregationFramework#Sort.toString"),
            fmp("reactivemongo.core.actors.LegacyDBSystem.lnm"),
            fmp("reactivemongo.core.actors.StandardDBSystem.lnm"),
            imt("reactivemongo.api.MongoConnection.sendExpectingResponse"), // priv
            imt("reactivemongo.api.collections.GenericCollection#BulkMaker.result"),
            imt("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand.result"),
            imt("reactivemongo.api.commands.AggregationFramework#Group.apply"),
            imt("reactivemongo.api.commands.AggregationFramework#Sort.this"),
            imt("reactivemongo.core.actors.AwaitingResponse.apply"), // private
            imt("reactivemongo.core.actors.AwaitingResponse.copy"), // private
            imt("reactivemongo.core.actors.AwaitingResponse.this"), // private
            imt("reactivemongo.core.actors.ChannelConnected.apply"), // private
            imt("reactivemongo.core.actors.ChannelConnected.copy"), // private
            imt("reactivemongo.core.actors.ChannelConnected.this"), // private
            imt("reactivemongo.core.actors.ChannelDisconnected.apply"), // private
            imt("reactivemongo.core.actors.ChannelDisconnected.copy"), // private
            imt("reactivemongo.core.actors.ChannelDisconnected.this"), // private
            imt("reactivemongo.core.nodeset.ChannelFactory.channelFactory"),
            imt("reactivemongo.core.nodeset.ChannelFactory.create"),
            imt("reactivemongo.core.nodeset.ChannelFactory.this"), // private
            imt("reactivemongo.core.nodeset.Connection.apply"),
            imt("reactivemongo.core.nodeset.Connection.channel"),
            imt("reactivemongo.core.nodeset.Connection.copy"),
            imt("reactivemongo.core.nodeset.Connection.send"),
            imt("reactivemongo.core.nodeset.Connection.this"),
            imt("reactivemongo.core.protocol.BufferAccessors#BufferInteroperable.apply"),
            imt("reactivemongo.core.protocol.ChannelBufferReadable.readFrom"),
            imt("reactivemongo.core.protocol.MessageHeader.apply"),
            imt("reactivemongo.core.protocol.MessageHeader.readFrom"),
            imt("reactivemongo.core.protocol.MongoHandler.channelClosed"),
            imt("reactivemongo.core.protocol.MongoHandler.channelConnected"),
            imt("reactivemongo.core.protocol.MongoHandler.channelDisconnected"),
            imt("reactivemongo.core.protocol.MongoHandler.exceptionCaught"),
            imt("reactivemongo.core.protocol.MongoHandler.log"),
            imt("reactivemongo.core.protocol.MongoHandler.messageReceived"),
            imt("reactivemongo.core.protocol.MongoHandler.writeComplete"),
            imt("reactivemongo.core.protocol.MongoHandler.writeRequested"),
            imt("reactivemongo.core.protocol.Reply.apply"),
            imt("reactivemongo.core.protocol.Reply.readFrom"),
            imt("reactivemongo.core.protocol.ReplyDocumentIterator.apply"),
            imt("reactivemongo.core.protocol.RequestEncoder.encode"),
            imt("reactivemongo.core.protocol.Response.apply"),
            imt("reactivemongo.core.protocol.Response.copy"),
            imt("reactivemongo.core.protocol.Response.documents"),
            imt("reactivemongo.core.protocol.Response.this"),
            imt("reactivemongo.core.protocol.ResponseDecoder.decode"),
            imt("reactivemongo.core.protocol.ResponseFrameDecoder.decode"),
            imt("reactivemongo.core.protocol.package.RichBuffer"),
            irt("reactivemongo.api.Cursor.logger"),
            irt("reactivemongo.api.MongoConnection.killed"),
            irt("reactivemongo.api.collections.GenericCollection#BulkMaker.result"),
            irt("reactivemongo.api.collections.GenericCollection#Mongo26WriteCommand.result"),
            irt("reactivemongo.api.commands.AggregationFramework#Aggregate.pipeline"),
            irt("reactivemongo.api.commands.AggregationFramework#Sort.apply"),
            irt("reactivemongo.api.commands.LastError.originalDocument"),
            irt("reactivemongo.core.actors.AwaitingResponse.channelID"), // priv
            irt("reactivemongo.core.actors.ChannelConnected.channelId"), // priv
            irt("reactivemongo.core.actors.ChannelDisconnected.channelId"), // prv
            irt("reactivemongo.core.nodeset.ChannelFactory.channelFactory"),
            irt("reactivemongo.core.nodeset.ChannelFactory.create"),
            irt("reactivemongo.core.nodeset.Connection.channel"),
            irt("reactivemongo.core.nodeset.Connection.send"),
            irt("reactivemongo.core.protocol.Response.documents"),
            mcp("reactivemongo.api.MongoConnection$MonitorActor$"),
            mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper"), // priv
            mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper$"), // priv
            mcp("reactivemongo.api.collections.GenericCollection$Mongo24BulkInsert"),
            mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions"),
            mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions$"),
            mcp("reactivemongo.api.commands.AggregationFramework$DocumentStage"),
            mcp("reactivemongo.api.commands.AggregationFramework$DocumentStageCompanion"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStage"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer$"),
            mcp("reactivemongo.api.commands.CursorCommand"),
            mcp("reactivemongo.core.actors.ChannelClosed"), // private
            mcp("reactivemongo.core.actors.ChannelClosed$"), // private
            mcp("reactivemongo.core.actors.ChannelUnavailable"), // private
            mcp("reactivemongo.core.actors.ChannelUnavailable$"), // private
            mcp("reactivemongo.core.actors.RefreshAllNodes"),
            mcp("reactivemongo.core.actors.RefreshAllNodes$"),
            mcp("reactivemongo.core.actors.RequestIds"),
            mcp("reactivemongo.core.netty.package"),
            mcp("reactivemongo.core.netty.package$"),
            mcp("reactivemongo.core.protocol.RequestEncoder$"), // private
            mmp("reactivemongo.api.CollectionMetaCommands.drop"),
            mmp("reactivemongo.api.DB.defaultReadPreference"),
            mmp("reactivemongo.api.DB.sister"),
            mmp("reactivemongo.api.DBMetaCommands.serverStatus"),
            mmp("reactivemongo.api.MongoConnection#MonitorActor.primaryAvailable_="),
            mmp("reactivemongo.api.MongoConnectionOptions.apply"),
            mmp("reactivemongo.api.MongoConnectionOptions.copy"),
            mmp("reactivemongo.api.MongoConnectionOptions.this"),
            mmp("reactivemongo.api.MongoDriver#AddConnection.apply"),
            mmp("reactivemongo.api.MongoDriver#AddConnection.copy"),
            mmp("reactivemongo.api.MongoDriver#AddConnection.this"),
            mmp("reactivemongo.api.collections.GenericQueryBuilder.copy"),
            mmp("reactivemongo.api.collections.bson.BSONQueryBuilder$"),
            mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.apply"),
            mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.copy"),
            mmp("reactivemongo.api.collections.bson.BSONQueryBuilder.this"),
            mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.copy"),
            mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.cursorOptions"),
            mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.needsCursor"),
            mmp("reactivemongo.api.commands.AggregationFramework#Aggregate.this"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.andThen"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.compose"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.copy"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.andThen"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.compose"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.copy"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#Group.this"),
            mmp("reactivemongo.api.commands.AggregationFramework#Limit.n"),
            mmp("reactivemongo.api.commands.AggregationFramework#Match.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Match.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#Project.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Project.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#Redact.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Redact.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#Skip.n"),
            mmp("reactivemongo.api.commands.AggregationFramework#Sort.apply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Sort.copy"),
            mmp("reactivemongo.api.commands.AggregationFramework#Sort.document"),
            mmp("reactivemongo.api.commands.AggregationFramework#Sort.unapply"),
            mmp("reactivemongo.api.commands.AggregationFramework#Unwind.prefixedField"),
            mmp("reactivemongo.api.commands.AggregationFramework.AggregateCursorOptions"),
            mmp("reactivemongo.api.commands.AggregationFramework.PipelineStageDocumentProducer"),
            mmp("reactivemongo.api.commands.CollStatsResult.apply"),
            mmp("reactivemongo.api.commands.CollStatsResult.this"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.apply"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.copy"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.this"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.apply"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.copy"),
            mmp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            mmp("reactivemongo.api.commands.GetLastError#TagSet.s"),
            mmp("reactivemongo.api.commands.LastError.apply"),
            mmp("reactivemongo.api.commands.LastError.copy"),
            mmp("reactivemongo.api.commands.LastError.this"),
            mmp("reactivemongo.api.gridfs.DefaultFileToSave.apply"),
            mmp("reactivemongo.api.indexes.Index.apply"),
            mmp("reactivemongo.api.indexes.Index.copy"),
            mmp("reactivemongo.api.indexes.Index.this"),
            mmp("reactivemongo.bson.BSONTimestamp.toString"),
            mmp("reactivemongo.core.actors.MongoDBSystem.DefaultConnectionRetryInterval"),
            mmp("reactivemongo.core.protocol.ChannelBufferReadable.apply"),
            mmp("reactivemongo.core.protocol.MongoHandler.this"), // private
            mtp("reactivemongo.api.FailoverStrategy$"),
            mtp("reactivemongo.api.MongoConnectionOptions$"),
            mtp("reactivemongo.api.MongoDriver$AddConnection$"),
            mtp("reactivemongo.api.ReadPreference$Taggable"), // 0.11.x
            mtp("reactivemongo.api.ReadPreference$Taggable$"), // 0.11.x
            mtp("reactivemongo.api.collections.bson.BSONQueryBuilder$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Aggregate"),
            mtp("reactivemongo.api.commands.AggregationFramework$GeoNear"),
            mtp("reactivemongo.api.commands.AggregationFramework$GeoNear$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Group"),
            mtp("reactivemongo.api.commands.AggregationFramework$Group$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Limit"),
            mtp("reactivemongo.api.commands.AggregationFramework$Match"),
            mtp("reactivemongo.api.commands.AggregationFramework$Match$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Out"),
            mtp("reactivemongo.api.commands.AggregationFramework$Project"),
            mtp("reactivemongo.api.commands.AggregationFramework$Project$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Redact"),
            mtp("reactivemongo.api.commands.AggregationFramework$Redact$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Skip"),
            mtp("reactivemongo.api.commands.AggregationFramework$Sort"),
            mtp("reactivemongo.api.commands.AggregationFramework$Sort$"),
            mtp("reactivemongo.api.commands.AggregationFramework$Unwind"),
            mtp("reactivemongo.api.commands.CollStatsResult$"),
            mtp("reactivemongo.api.commands.DefaultWriteResult"),
            mtp("reactivemongo.api.commands.LastError$"),
            mtp("reactivemongo.api.commands.UpdateWriteResult"),
            mtp("reactivemongo.api.commands.WriteResult"),
            mtp("reactivemongo.api.gridfs.DefaultFileToSave"),
            mtp("reactivemongo.api.gridfs.DefaultFileToSave$"),
            mtp("reactivemongo.api.indexes.Index$"),
            mtp("reactivemongo.core.actors.ChannelDisconnected"), // private
            mtp("reactivemongo.core.actors.RequestIdGenerator"),
            mtp("reactivemongo.core.actors.RequestIdGenerator$"),
            mtp("reactivemongo.core.nodeset.Authenticating$"),
            mtp("reactivemongo.core.protocol.MongoHandler"),
            mtp("reactivemongo.core.protocol.RequestEncoder"),
            mtp("reactivemongo.core.protocol.ResponseDecoder"),
            mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
            mtp("reactivemongo.core.protocol.ResponseInfo"),
            mtp("reactivemongo.core.protocol.ResponseInfo$"), // private
            x[AbstractClassProblem]("reactivemongo.core.protocol.Response"),
            x[DirectAbstractMethodProblem]("reactivemongo.api.Cursor.headOption"),
            x[DirectAbstractMethodProblem]("reactivemongo.api.collections.GenericQueryBuilder.readPreference"),
            x[IncompatibleTemplateDefProblem]("reactivemongo.api.commands.DeleteCommand$DeleteElement"),
            x[IncompatibleTemplateDefProblem]("reactivemongo.core.actors.MongoDBSystem"),
            x[IncompatibleTemplateDefProblem]("reactivemongo.core.nodeset.Authenticating")
          )
        },
        Common.closeableObject in Test := "tests.Common$",
        testOptions in Test += Tests.Cleanup(Common.cleanup.value),
        mappings in (Compile, packageBin) ~= driverFilter,
        //mappings in (Compile, packageDoc) ~= driverFilter,
        mappings in (Compile, packageSrc) ~= driverFilter,
        apiMappings ++= Documentation.mappings("com.typesafe.akka", "http://doc.akka.io/api/akka/%s/")("akka-actor").value ++ Documentation.mappings("com.typesafe.play", "http://playframework.com/documentation/%s/api/scala/index.html", _.replaceAll("[\\d]$", "x"))("play-iteratees").value
      )
    ).configure { p =>
      sys.props.get("test.nettyNativeArch") match {
        case Some("osx") => p.settings(Seq(
          compile in Test := (compile in Test).
            dependsOn(osxShaded / Compile / packageBin).value,
          unmanagedJars in Test += {
            val dir = (target in osxShaded).value
            val jar = (assemblyJarName in (osxShaded, assembly)).value

            (dir / "classes").mkdirs() // Findbugs workaround

            Attributed(dir / jar)(AttributeMap.empty)
          }
        ))

        case Some(_/* linux */) => p.settings(Seq(
          compile in Test := (compile in Test).
            dependsOn(linuxShaded / Compile / packageBin).value,
          unmanagedJars in Test += {
            val dir = (target in linuxShaded).value
            val jar = (assemblyJarName in (linuxShaded, assembly)).value

            (dir / "classes").mkdirs() // Findbugs workaround

            Attributed(dir / jar)(AttributeMap.empty)
          }
        ))

        case _ => p
      }
    }.dependsOn(shaded)

  // ---

  private val driverFilter: Seq[(File, String)] => Seq[(File, String)] = {
    (_: Seq[(File, String)]).filter {
      case (file, name) =>
        !(name endsWith "external/reactivemongo/StaticListenerBinder.class")
    }
  } andThen Common.filter

  private val driverCleanup = taskKey[Unit]("Driver compilation cleanup")
}
