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

  /** The Scala major version (e.g. 2.12) */
  val scalaBinaryVersion = "${scalaBinaryVersion.value}"
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

          @inline def dam(s: String) = x[DirectAbstractMethodProblem](s)
          @inline def mmp(s: String) = x[MissingMethodProblem](s)
          @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
          @inline def fmp(s: String) = x[FinalMethodProblem](s)
          @inline def fcp(s: String) = x[FinalClassProblem](s)
          @inline def irt(s: String) = x[IncompatibleResultTypeProblem](s)
          @inline def mtp(s: String) = x[MissingTypesProblem](s)
          @inline def mcp(s: String) = x[MissingClassProblem](s)
          @inline def isp(s: String) = x[IncompatibleSignatureProblem](s)

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
            x[IncompatibleTemplateDefProblem]("reactivemongo.core.nodeset.Authenticating"),
            // MiMa 0.5.0
            ProblemFilters.exclude[StaticVirtualMemberProblem]("java.lang.Object.hashCode"),
            ProblemFilters.exclude[StaticVirtualMemberProblem]("java.lang.Object.toString"),
            isp("reactivemongo.core.actors.ChannelConnected.unapply"),
            isp("reactivemongo.core.actors.AwaitingResponse.unapply"),
            isp("reactivemongo.core.actors.ChannelDisconnected.unapply"),
            isp("reactivemongo.core.actors.ChannelDisconnected.compose"),
            isp("reactivemongo.core.actors.ChannelDisconnected.andThen"),
            isp("reactivemongo.core.actors.AwaitingResponse.unapply"),
            isp("reactivemongo.core.actors.AwaitingResponse.curried"),
            isp("reactivemongo.core.actors.AwaitingResponse.tupled"),
            isp("reactivemongo.core.actors.AwaitingResponse.retriable"),
            isp("reactivemongo.core.actors.ChannelConnected.unapply"),
            isp("reactivemongo.core.actors.ChannelConnected.compose"),
            isp("reactivemongo.core.actors.ChannelConnected.andThen"),
            isp("reactivemongo.core.actors.ChannelDisconnected.unapply"),
            isp("reactivemongo.core.netty.BufferSequence.unapplySeq"),
            isp("reactivemongo.core.netty.BufferSequence.unapplySeq"),
            isp("reactivemongo.core.protocol.Request.<init>$default$6"),
            isp("reactivemongo.core.protocol.Request.copy$default$6"),
            isp("reactivemongo.core.protocol.Request.unapply"),
            isp("reactivemongo.core.protocol.Request.writeTo"),
            isp("reactivemongo.core.protocol.Request.channelIdHint"),
            isp("reactivemongo.core.protocol.Request.copy"),
            isp("reactivemongo.core.protocol.Request.apply$default$6"),
            isp("reactivemongo.core.protocol.Request.apply"),
            isp("reactivemongo.core.protocol.Request.this"),
            isp("reactivemongo.core.protocol.Request.<init>$default$6"),
            isp("reactivemongo.core.protocol.Request.unapply"),
            isp("reactivemongo.core.protocol.Request.apply$default$6"),
            isp("reactivemongo.core.protocol.Request.apply"),
            isp("reactivemongo.core.protocol.KillCursors.writeTo"),
            isp("reactivemongo.core.protocol.Update.writeTo"),
            isp("reactivemongo.core.protocol.Query.writeTo"),
            isp("reactivemongo.core.protocol.MessageHeader.writeTo"),
            isp("reactivemongo.core.protocol.ChannelBufferWritable.writeTo"),
            isp("reactivemongo.core.protocol.Delete.writeTo"),
            isp("reactivemongo.core.protocol.Response.unapply"),
            isp("reactivemongo.core.protocol.GetMore.writeTo"),
            isp("reactivemongo.core.protocol.Response.unapply"),
            isp("reactivemongo.core.protocol.RequestMaker.unapply"),
            isp("reactivemongo.core.protocol.RequestMaker.channelIdHint"),
            isp("reactivemongo.core.protocol.RequestMaker.curried"),
            isp("reactivemongo.core.protocol.RequestMaker.apply$default$4"),
            isp("reactivemongo.core.protocol.RequestMaker.tupled"),
            isp("reactivemongo.core.protocol.RequestMaker.copy"),
            isp("reactivemongo.core.protocol.RequestMaker.<init>$default$4"),
            isp("reactivemongo.core.protocol.RequestMaker.copy$default$4"),
            isp("reactivemongo.core.protocol.RequestMaker.this"),
            isp("reactivemongo.core.protocol.Insert.writeTo"),
            isp("reactivemongo.core.protocol.ResponseInfo.compose"),
            isp("reactivemongo.core.protocol.ResponseInfo.andThen"),
            isp("reactivemongo.core.protocol.RequestMaker.unapply"),
            isp("reactivemongo.core.protocol.RequestMaker.apply$default$4"),
            isp("reactivemongo.core.protocol.RequestMaker.apply"),
            isp("reactivemongo.core.protocol.RequestMaker.<init>$default$4"),
            isp("reactivemongo.core.nodeset.Connection.curried"),
            isp("reactivemongo.core.nodeset.Authenticating.unapply"),
            isp("reactivemongo.core.nodeset.Authenticate.unapply"),
            isp("reactivemongo.core.nodeset.Authenticate.unapply"),
            isp("reactivemongo.core.nodeset.Authenticate.curried"),
            isp("reactivemongo.core.nodeset.Authenticate.tupled"),
            isp("reactivemongo.core.nodeset.Authenticating.unapply"),
            isp("reactivemongo.api.BSONSerializationPack.serializeAndWrite"),
            isp("reactivemongo.api.BSONSerializationPack.readAndDeserialize"),
            isp("reactivemongo.api.ReadPreference#Nearest.unapply"),
            isp("reactivemongo.api.ReadPreference#SecondaryPreferred.unapply"),
            isp("reactivemongo.api.ReadPreference#Secondary.unapply"),
            isp("reactivemongo.api.ReadPreference#PrimaryPreferred.unapply"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.unapply"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.apply"),
            isp("reactivemongo.api.DefaultCursor#Impl.collect"),
            isp("reactivemongo.api.BSONSerializationPack.readAndDeserialize"),
            isp("reactivemongo.api.MongoConnection.probe"),
            isp("reactivemongo.api.MongoConnection.waitIsAvailable"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.unapply"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.apply"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.result"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.copy"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.copy$default$1"),
            isp("reactivemongo.api.MongoConnection#IsPrimaryAvailable.this"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.result"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.copy"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.copy$default$1"),
            isp("reactivemongo.api.MongoConnection#IsAvailable.this"),
            isp("reactivemongo.api.collections.Aggregator#AggregatorContext.otherOperators"),
            isp("reactivemongo.api.collections.Aggregator#AggregatorContext.prepared"),
            isp("reactivemongo.api.collections.GenericCollection.aggregateWith1"),
            isp("reactivemongo.api.collections.GenericCollection.aggregatorContext$default$2"),
            isp("reactivemongo.api.collections.bson.BSONCollection.distinct$default$2"),
            isp("reactivemongo.api.collections.bson.BSONCollection.distinct"),
            isp("reactivemongo.api.collections.bson.BSONCollection.aggregatorContext$default$2"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#UpdateLastError.copy"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.unapply"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.apply"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.lastError"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.result"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.copy"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.copy$default$1"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModifyResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.unapply"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.apply"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.copy"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.copy$default$1"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.pipeline"),
            isp("reactivemongo.api.commands.AggregationFramework#Aggregate.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#UpdateLastError.unapply"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#UpdateLastError.apply"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.afterRead"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.beforeRead"),
            isp("reactivemongo.api.MongoDriver#AddConnection.unapply"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$4"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove$default$3"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$3"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$5"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove$default$2"),
            isp("reactivemongo.api.collections.bson.BSONQueryBuilder.unapply"),
            isp("reactivemongo.api.collections.bson.BSONQueryBuilder.curried"),
            isp("reactivemongo.api.collections.bson.BSONQueryBuilder.tupled"),
            isp("reactivemongo.api.collections.bson.BSONQueryBuilder.unapply"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.compose"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.andThen"),
            isp("reactivemongo.api.commands.LastError.unapply"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.unapply"),
            isp("reactivemongo.api.commands.CollStatsResult.unapply"),
            isp("reactivemongo.api.commands.InsertCommand#Insert.unapply"),
            isp("reactivemongo.api.commands.Upserted.unapply"),
            isp("reactivemongo.api.commands.Upserted.curried"),
            isp("reactivemongo.api.commands.Upserted.tupled"),
            isp("reactivemongo.api.commands.Command.defaultCursorFetcher"),
            isp("reactivemongo.api.commands.CollStatsResult.unapply"),
            isp("reactivemongo.api.commands.CollStatsResult.curried"),
            isp("reactivemongo.api.commands.CollStatsResult.tupled"),
            isp("reactivemongo.api.commands.LastError.upserted"),
            isp("reactivemongo.api.commands.LastError.unapply"),
            isp("reactivemongo.api.commands.LastError.curried"),
            isp("reactivemongo.api.commands.LastError.tupled"),
            isp("reactivemongo.api.commands.LastError.copy$default$8"),
            isp("reactivemongo.api.commands.AggregationFramework#Redact.compose"),
            isp("reactivemongo.api.commands.AggregationFramework#Redact.andThen"),
            isp("reactivemongo.api.commands.Upserted.unapply"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.compose"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.andThen"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.compose"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.andThen"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.unapply"),
            isp("reactivemongo.api.indexes.Index.unapply"),
            isp("reactivemongo.api.indexes.Index.curried"),
            isp("reactivemongo.api.indexes.Index.tupled"),
            isp("reactivemongo.api.indexes.Index.unapply"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$5"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.unapply"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$4"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$3"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$2"),
            isp("reactivemongo.api.gridfs.DefaultReadFile.unapply"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$5"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.unapply"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$4"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$3"),
            isp("reactivemongo.api.gridfs.DefaultFileToSave.apply$default$2"),
            isp("reactivemongo.api.gridfs.GridFS.readToOutputStream"),
            isp("reactivemongo.api.gridfs.GridFS.enumerate"),
            isp("reactivemongo.api.gridfs.GridFS.remove"),
            isp("reactivemongo.api.gridfs.GridFS.remove"),
            isp("reactivemongo.api.gridfs.GridFS.writeFromInputStream"),
            isp("reactivemongo.api.gridfs.GridFS.save"),
            isp("reactivemongo.api.gridfs.GridFS.iteratee"),
            isp("reactivemongo.api.gridfs.DefaultReadFile.unapply"),
            isp("reactivemongo.api.gridfs.DefaultReadFile.curried"),
            isp("reactivemongo.api.gridfs.DefaultReadFile.tupled"),
            isp("reactivemongo.core.commands.Aggregate.apply"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove"),
            isp("reactivemongo.api.collections.bson.BSONCollection.find"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.readTry"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.read"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.readOpt"),
            isp("reactivemongo.api.gridfs.GridFS.find"),
            fcp("reactivemongo.core.nodeset.ContinuousIterator"),
            isp("reactivemongo.api.FoldResponses#ProcResponses.this"),
            isp("reactivemongo.api.MongoDriver#AddConnection.this"),
            isp("reactivemongo.api.FoldResponses#OnError.this"),
            isp("reactivemongo.api.FoldResponses#ProcNext.this"),
            isp("reactivemongo.api.FoldResponses#ProcNext.this"),
            isp("reactivemongo.api.FoldResponses#HandleResponse.this"),
            isp("reactivemongo.api.Cursor#Fail.this"),
            isp("reactivemongo.api.FoldResponses#ProcResponses.this"),
            isp("reactivemongo.api.FoldResponses#OnError.this"),
            isp("reactivemongo.api.Driver#AddConnection.this"),
            isp("reactivemongo.api.FoldResponses#HandleResponse.this"),
            isp("reactivemongo.api.collections.UpdateOps#UnorderedUpdate.this"),
            isp("reactivemongo.api.collections.Aggregator#Aggregator.this"),
            isp("reactivemongo.api.collections.UpdateOps#OrderedUpdate.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Push.this"),
            isp("reactivemongo.api.commands.GroupAggregation#GroupFunction.this"),
            isp("reactivemongo.api.commands.CountCommand#Count.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Limit.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSamp.this"),
            isp("reactivemongo.api.commands.CountCommand#HintDocument.this"),
            isp("reactivemongo.api.commands.DeleteCommand#Delete.this"),
            isp("reactivemongo.api.commands.AggregationFramework#BucketAuto.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.DistinctCommand#DistinctResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Lookup.this"),
            isp("reactivemongo.api.commands.CountCommand#CountResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            isp("reactivemongo.api.commands.DistinctCommand#Distinct.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Descending.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind#Full.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Skip.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Sum.this"),
            isp("reactivemongo.api.commands.AggregationFramework#TextScore.this"),
            fcp("reactivemongo.core.nodeset.ContinuousIterator"),
            isp("reactivemongo.api.FoldResponses#ProcResponses.this"),
            isp("reactivemongo.api.MongoDriver#AddConnection.this"),
            isp("reactivemongo.api.FoldResponses#OnError.this"),
            isp("reactivemongo.api.FoldResponses#ProcNext.this"),
            isp("reactivemongo.api.FoldResponses#ProcNext.this"),
            isp("reactivemongo.api.FoldResponses#HandleResponse.this"),
            isp("reactivemongo.api.Cursor#Fail.this"),
            isp("reactivemongo.api.FoldResponses#ProcResponses.this"),
            isp("reactivemongo.api.FoldResponses#OnError.this"),
            isp("reactivemongo.api.Driver#AddConnection.this"),
            isp("reactivemongo.api.FoldResponses#HandleResponse.this"),
            isp("reactivemongo.api.collections.UpdateOps#UnorderedUpdate.this"),
            isp("reactivemongo.api.collections.Aggregator#Aggregator.this"),
            isp("reactivemongo.api.collections.UpdateOps#OrderedUpdate.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Push.this"),
            isp("reactivemongo.api.commands.GroupAggregation#GroupFunction.this"),
            isp("reactivemongo.api.commands.CountCommand#Count.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Limit.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSamp.this"),
            isp("reactivemongo.api.commands.CountCommand#HintDocument.this"),
            isp("reactivemongo.api.commands.DeleteCommand#Delete.this"),
            isp("reactivemongo.api.commands.AggregationFramework#BucketAuto.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.DistinctCommand#DistinctResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Lookup.this"),
            isp("reactivemongo.api.commands.CountCommand#CountResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            isp("reactivemongo.api.commands.DistinctCommand#Distinct.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Descending.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind#Full.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Skip.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Sum.this"),
            isp("reactivemongo.api.commands.AggregationFramework#TextScore.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupMulti.this"),
            isp("reactivemongo.api.commands.GroupAggregation#MaxField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#FirstField.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#FindAndModify.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Push.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumField.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Remove.this"),
            isp("reactivemongo.api.commands.CountCommand#CountResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddToSet.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Last.this"),
            isp("reactivemongo.api.commands.AggregationFramework#BucketAuto.this"),
            isp("reactivemongo.api.commands.AggregationFramework#UnwindField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Redact.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddToSet.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Descending.this"),
            isp("reactivemongo.api.commands.GroupAggregation#LastField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Group.this"),
            isp("reactivemongo.api.commands.CreateUserCommand#CreateUser.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Min.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSampField.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#IsMaster.this"),
            isp("reactivemongo.api.commands.GroupAggregation#LastField.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            isp("reactivemongo.api.commands.AggregationFramework#AggregationResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumValue.this"),
            isp("reactivemongo.api.commands.GroupAggregation#MinField.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#IsMasterResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Avg.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumValue.this"),
            isp("reactivemongo.api.commands.UpdateCommand#Update.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumAll.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Cursor.this"),
            isp("reactivemongo.api.commands.GroupAggregation#PushField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Cursor.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSampField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Lookup.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AvgField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#MinField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind#Full.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStatAccesses.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Limit.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStatsResult.this"),
            isp("reactivemongo.api.commands.InsertCommand#Insert.this"),
            isp("reactivemongo.api.commands.DeleteCommand#Delete.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Out.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupMulti.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.this"),
            isp("reactivemongo.api.commands.CreateUserCommand#CreateUser.this"),
            isp("reactivemongo.api.commands.AggregationFramework#MetadataSort.this"),
            isp("reactivemongo.api.commands.UpdateCommand#Update.this"),
            isp("reactivemongo.api.commands.CountCommand#Hint.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFields.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Ascending.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#ReplicaSet.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#ReplicaSet.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPopField.this"),
            isp("reactivemongo.api.commands.InsertCommand#Insert.this"),
            isp("reactivemongo.api.commands.AggregationFramework#MetadataSort.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Last.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#IsMasterResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Redact.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Sum.this"),
            isp("reactivemongo.api.commands.GroupAggregation#First.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sample.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPopField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#First.this"),
            isp("reactivemongo.api.commands.UpdateCommand#UpdateElement.this"),
            isp("reactivemongo.api.commands.AggregationFramework#AggregationResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Ascending.this"),
            isp("reactivemongo.api.commands.CountCommand#Count.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Min.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GraphLookup.this"),
            isp("reactivemongo.api.commands.DistinctCommand#DistinctResult.this"),
            isp("reactivemongo.api.commands.IsMasterCommand#ReplicaSet.this"),
            isp("reactivemongo.api.commands.CountCommand#HintString.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Avg.this"),
            isp("reactivemongo.api.commands.GroupAggregation#FirstField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStatAccesses.this"),
            isp("reactivemongo.api.commands.ImplicitCommandHelpers#ImplicitlyDocumentProducer.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPop.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Out.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStats.this"),
            isp("reactivemongo.api.commands.Command#CommandWithPackRunner#RawCommand.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.this"),
            isp("reactivemongo.api.commands.GroupAggregation#PushField.this"),
            isp("reactivemongo.api.commands.CountCommand#HintDocument.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFields.this"),
            isp("reactivemongo.api.commands.CountCommand#HintString.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Filter.this"),
            isp("reactivemongo.api.commands.DistinctCommand#Distinct.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSamp.this"),
            isp("reactivemongo.api.commands.AggregationFramework#UnwindField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFieldToSet.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Group.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GraphLookup.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStatsResult.this"),
            isp("reactivemongo.api.commands.Command#CommandWithPackRunner#RawCommand.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Filter.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AvgField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFieldToSet.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sample.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Skip.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPop.this"),
            isp("reactivemongo.api.commands.GroupAggregation#MaxField.this"),
            isp("reactivemongo.api.commands.DeleteCommand#DeleteElement.this"),
            dam("reactivemongo.core.nodeset.RoundRobiner.pick"),
            dam("reactivemongo.core.nodeset.RoundRobiner.pickWithFilter"),
            isp("reactivemongo.core.commands.ScramSha1Initiate.apply"),
            isp("reactivemongo.core.commands.ScramSha1Initiate.parseResponse"),
            isp("reactivemongo.core.commands.ScramSha1StartNegociation.data"),
            x[StaticVirtualMemberProblem]("reactivemongo.core.commands.ScramSha1StartNegociation.keyFactory")
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
