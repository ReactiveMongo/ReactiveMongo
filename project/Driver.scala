import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaBinaryIssueFilters

import com.github.sbt.cpd.CpdPlugin

import sbtassembly.AssemblyKeys, AssemblyKeys._

import com.github.sbt.findbugs.FindbugsKeys.findbugsAnalyzedPath

final class Driver(
  bsonmacros: Project,
  shaded: Project) {

  import Dependencies._

  lazy val module = Project("ReactiveMongo", file("driver")).
    enablePlugins(CpdPlugin).
    dependsOn(bsonmacros, shaded).
    settings(
      BuildSettings.settings ++ Findbugs.settings ++ Seq(
        resolvers := Resolvers.resolversList,
        compile in Compile := (compile in Compile).
          dependsOn(assembly in shaded).value,
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
        driverCleanup := driverCleanup.triggeredBy(compile in Compile).value,
        unmanagedJars in Compile := {
          val shadedDir = (target in shaded).value
          val shadedJar = (assemblyJarName in (shaded, assembly)).value

          (shadedDir / "classes").mkdirs() // Findbugs workaround

          Seq(Attributed(shadedDir / shadedJar)(AttributeMap.empty))
        },
        libraryDependencies ++= akka.value ++ Seq(
          "org.reactivemongo" % "reactivemongo-native" % version.value % Test,
          "dnsjava" % "dnsjava" % "2.1.8",
          playIteratees.value, commonsCodec,
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
            fmp("reactivemongo.core.actors.LegacyDBSystem.lnm"),
            fmp("reactivemongo.core.actors.StandardDBSystem.lnm"),
            x[DirectAbstractMethodProblem](
              "reactivemongo.api.Cursor.headOption"),
            x[DirectAbstractMethodProblem](
              "reactivemongo.api.collections.GenericQueryBuilder.readPreference"),
            // --
            x[IncompatibleTemplateDefProblem](
              "reactivemongo.api.commands.DeleteCommand$DeleteElement"),
            mcp("reactivemongo.api.MongoConnection$MonitorActor$"),
            imt("reactivemongo.api.MongoConnection.sendExpectingResponse"), // priv
            mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper$"), // priv
            mcp("reactivemongo.api.ReadPreference$BSONDocumentWrapper"), // priv
            mtp("reactivemongo.api.ReadPreference$Taggable"), // 0.11.x
            mtp("reactivemongo.api.ReadPreference$Taggable$"), // 0.11.x
            fcp("reactivemongo.api.MongoDriver$SupervisorActor"), // private
            mcp("reactivemongo.api.MongoDriver$SupervisorActor$"), // private
            fcp("reactivemongo.api.collections.bson.BSONCollection"),
            imt("reactivemongo.core.actors.AwaitingResponse.apply"), // private
            imt("reactivemongo.core.actors.AwaitingResponse.this"), // private
            mmp("reactivemongo.core.protocol.MongoHandler.this"), // private
            fcp("reactivemongo.core.nodeset.ChannelFactory"),
            imt("reactivemongo.core.nodeset.ChannelFactory.this"), // private
            mcp("reactivemongo.core.actors.RefreshAllNodes"),
            mcp("reactivemongo.core.actors.RefreshAllNodes$"),
            imt("reactivemongo.core.actors.ChannelConnected.apply"), // private
            mcp("reactivemongo.core.actors.ChannelUnavailable$"), // private
            mcp("reactivemongo.core.actors.ChannelUnavailable"), // private
            mtp("reactivemongo.core.actors.ChannelDisconnected"), // private
            imt("reactivemongo.core.actors.ChannelDisconnected.copy"), // private
            imt("reactivemongo.core.actors.ChannelDisconnected.this"), // private
            irt("reactivemongo.core.actors.ChannelDisconnected.channelId"), // prv
            mcp("reactivemongo.core.actors.ChannelClosed$"), // private
            mcp("reactivemongo.core.actors.ChannelClosed"), // private
            imt("reactivemongo.core.actors.AwaitingResponse.copy"), // private
            irt("reactivemongo.core.actors.AwaitingResponse.channelID"), // priv
            imt("reactivemongo.core.actors.ChannelConnected.copy"), // private
            imt("reactivemongo.core.actors.ChannelConnected.this"), // private
            irt("reactivemongo.core.actors.ChannelConnected.channelId"), // priv
            imt("reactivemongo.core.actors.ChannelDisconnected.apply"), // private
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
            mcp("reactivemongo.core.protocol.RequestEncoder$"), // private
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
            fmp("reactivemongo.api.collections.bson.BSONCollection.fullCollectionName"),
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
            fcp("reactivemongo.core.nodeset.ChannelFactory"),
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
            mmp("reactivemongo.api.indexes.Index.apply"),
            mcp("reactivemongo.core.commands.Count"),
            mcp("reactivemongo.core.commands.CollStats"),
            mcp("reactivemongo.core.commands.RenameCollection$"),
            mcp("reactivemongo.core.commands.ConvertToCapped"),
            mcp("reactivemongo.core.commands.LastError$"),
            mcp("reactivemongo.core.commands.LastError"),
            mcp("reactivemongo.core.commands.GetLastError"),
            mcp("reactivemongo.core.commands.IsMaster$"),
            mcp("reactivemongo.core.commands.CollStats$"),
            mcp("reactivemongo.core.commands.CreateCollection$"),
            mcp("reactivemongo.core.commands.IsMasterResponse$"),
            mcp("reactivemongo.core.commands.Count$"),
            mcp("reactivemongo.core.commands.IsMasterResponse"),
            mcp("reactivemongo.core.commands.CreateCollection"),
            mcp("reactivemongo.core.commands.EmptyCapped"),
            mcp("reactivemongo.core.commands.RawCommand"),
            mcp("reactivemongo.core.commands.GetLastError$"),
            mcp("reactivemongo.core.commands.Drop"),
            mcp("reactivemongo.core.commands.RawCommand$"),
            mcp("reactivemongo.core.commands.RenameCollection"),
            mcp("reactivemongo.core.commands.IsMaster"),
            mcp("reactivemongo.core.commands.DeleteIndex$"),
            mcp("reactivemongo.core.commands.Update"),
            mcp("reactivemongo.core.commands.FindAndModify"),
            mcp("reactivemongo.core.commands.Update$"),
            mcp("reactivemongo.core.commands.FindAndModify$"),
            mcp("reactivemongo.core.commands.Remove$"),
            mcp("reactivemongo.core.commands.Remove"),
            mcp("reactivemongo.core.commands.DeleteIndex"),
            x[AbstractClassProblem]("reactivemongo.core.protocol.Response")
          )
        },
        testOptions in Test += Tests.Cleanup(BuildSettings.commonCleanup.value),
        mappings in (Compile, packageBin) ~= driverFilter,
        //mappings in (Compile, packageDoc) ~= driverFilter,
        mappings in (Compile, packageSrc) ~= driverFilter,
        apiMappings ++= Documentation.mappings("com.typesafe.akka", "http://doc.akka.io/api/akka/%s/")("akka-actor").value ++ Documentation.mappings("com.typesafe.play", "http://playframework.com/documentation/%s/api/scala/index.html", _.replaceAll("[\\d]$", "x"))("play-iteratees").value
      )
    )

  // ---

  private val driverFilter: Seq[(File, String)] => Seq[(File, String)] = {
    (_: Seq[(File, String)]).filter {
      case (file, name) =>
        !(name endsWith "external/reactivemongo/StaticListenerBinder.class")
    }
  } andThen BuildSettings.filter

  private val driverCleanup = taskKey[Unit]("Driver compilation cleanup")
}
