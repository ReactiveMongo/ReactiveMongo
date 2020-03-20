import scala.xml.{
  Attribute => XmlAttr,
  Elem => XmlElem,
  Node => XmlNode,
  NodeSeq,
  XML
}

import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaBinaryIssueFilters

import com.github.sbt.cpd.CpdPlugin

import com.github.sbt.findbugs.FindbugsKeys.findbugsAnalyzedPath

final class Driver(
  bson: Project,
  bsonmacros: Project,
  core: Project,
  bsonCompat: Project
) {
  import Dependencies._
  import XmlUtil._

  lazy val module = Project("ReactiveMongo", file("driver")).
    enablePlugins(CpdPlugin).
    settings(Findbugs.settings ++ Seq(
        unmanagedSourceDirectories in Compile ++= {
          val v = scalaBinaryVersion.value

          if (v == "2.11" || v == "2.12") {
            Seq((sourceDirectory in Compile).value / "scala-2.11_12")
          } else {
            Seq.empty[File]
          }
        },
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
        libraryDependencies ++= {
          if (scalaBinaryVersion.value != "2.13") {
            Seq(playIteratees.value)
          } else {
            Seq.empty
          }
        },
        libraryDependencies ++= {
          if (!Common.useShaded.value) {
            Seq(Dependencies.netty % Provided)
          } else {
            Seq.empty[ModuleID]
          }
        },
        libraryDependencies ++= akka.value ++ Seq(
          "dnsjava" % "dnsjava" % "3.0.2",
          commonsCodec,
          shapelessTest % Test, specs.value) ++ logApi,
        findbugsAnalyzedPath += target.value / "external",
        mimaBinaryIssueFilters ++= {
          import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

          @inline def mmp(s: String) = x[MissingMethodProblem](s)
          @inline def imt(s: String) = x[IncompatibleMethTypeProblem](s)
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
            imt("reactivemongo.api.commands.AggregationFramework#Group.apply"),
            irt("reactivemongo.api.commands.AggregationFramework#Aggregate.pipeline"),
            mcp("reactivemongo.api.MongoConnection$MonitorActor$"),
            mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions"),
            mcp("reactivemongo.api.commands.AggregationFramework$AggregateCursorOptions$"),
            mcp("reactivemongo.api.commands.AggregationFramework$DocumentStage"),
            mcp("reactivemongo.api.commands.AggregationFramework$DocumentStageCompanion"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStage"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer"),
            mcp("reactivemongo.api.commands.AggregationFramework$PipelineStageDocumentProducer$"),
            mcp("reactivemongo.api.commands.CursorCommand"),
            mmp("reactivemongo.api.CollectionMetaCommands.drop"),
            mmp("reactivemongo.api.DB.defaultReadPreference"),
            mmp("reactivemongo.api.DB.sister"),
            mmp("reactivemongo.api.DBMetaCommands.serverStatus"),
            mmp("reactivemongo.api.MongoConnection#MonitorActor.primaryAvailable_="),
            mmp("reactivemongo.api.MongoConnectionOptions.apply"),
            mmp("reactivemongo.api.MongoConnectionOptions.copy"),
            mmp("reactivemongo.api.MongoConnectionOptions.this"),
            mmp("reactivemongo.api.collections.GenericQueryBuilder.copy"),
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
            mmp("reactivemongo.api.indexes.Index.apply"),
            mmp("reactivemongo.api.indexes.Index.copy"),
            mmp("reactivemongo.api.indexes.Index.this"),
            mmp("reactivemongo.core.actors.MongoDBSystem.DefaultConnectionRetryInterval"),
            mtp("reactivemongo.api.MongoConnectionOptions$"),
            mtp("reactivemongo.api.ReadPreference$Taggable"), // 0.11.x
            mtp("reactivemongo.api.ReadPreference$Taggable$"), // 0.11.x
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
            mtp("reactivemongo.api.commands.UpdateWriteResult"),
            mtp("reactivemongo.api.commands.WriteResult"),
            mtp("reactivemongo.api.indexes.Index$"),
            x[DirectAbstractMethodProblem]("reactivemongo.api.Cursor.headOption"),
            x[DirectAbstractMethodProblem]("reactivemongo.api.collections.GenericQueryBuilder.readPreference"),
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
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$4"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove$default$3"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$3"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update$default$5"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove$default$2"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.compose"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.andThen"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.unapply"),
            isp("reactivemongo.api.commands.CollStatsResult.unapply"),
            isp("reactivemongo.api.commands.Upserted.unapply"),
            isp("reactivemongo.api.commands.Upserted.curried"),
            isp("reactivemongo.api.commands.Upserted.tupled"),
            isp("reactivemongo.api.commands.Command.defaultCursorFetcher"),
            isp("reactivemongo.api.commands.CollStatsResult.unapply"),
            isp("reactivemongo.api.commands.CollStatsResult.curried"),
            isp("reactivemongo.api.commands.CollStatsResult.tupled"),
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
            isp("reactivemongo.api.gridfs.GridFS.readToOutputStream"),
            isp("reactivemongo.api.gridfs.GridFS.enumerate"),
            isp("reactivemongo.api.gridfs.GridFS.remove"),
            isp("reactivemongo.api.gridfs.GridFS.remove"),
            isp("reactivemongo.api.gridfs.GridFS.writeFromInputStream"),
            isp("reactivemongo.api.gridfs.GridFS.save"),
            isp("reactivemongo.api.gridfs.GridFS.iteratee"),
            isp("reactivemongo.api.collections.bson.BSONCollection.update"),
            isp("reactivemongo.api.collections.bson.BSONCollection.remove"),
            isp("reactivemongo.api.collections.bson.BSONCollection.find"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.readTry"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.read"),
            isp("reactivemongo.api.commands.bson.BSONFindAndModifyImplicits#FindAndModifyResultReader.readOpt"),
            isp("reactivemongo.api.gridfs.GridFS.find"),
            isp("reactivemongo.api.Cursor#Fail.this"),
            isp("reactivemongo.api.collections.UpdateOps#UnorderedUpdate.this"),
            isp("reactivemongo.api.collections.Aggregator#Aggregator.this"),
            isp("reactivemongo.api.collections.UpdateOps#OrderedUpdate.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Push.this"),
            isp("reactivemongo.api.commands.GroupAggregation#GroupFunction.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Limit.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSamp.this"),
            isp("reactivemongo.api.commands.AggregationFramework#BucketAuto.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Lookup.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Descending.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind#Full.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Skip.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Sum.this"),
            isp("reactivemongo.api.commands.AggregationFramework#TextScore.this"),
            isp("reactivemongo.api.Cursor#Fail.this"),
            isp("reactivemongo.api.collections.UpdateOps#UnorderedUpdate.this"),
            isp("reactivemongo.api.collections.Aggregator#Aggregator.this"),
            isp("reactivemongo.api.collections.UpdateOps#OrderedUpdate.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Push.this"),
            isp("reactivemongo.api.commands.GroupAggregation#GroupFunction.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Limit.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevSamp.this"),
            isp("reactivemongo.api.commands.AggregationFramework#BucketAuto.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Lookup.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
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
            isp("reactivemongo.api.commands.GroupAggregation#LastField.this"),
            isp("reactivemongo.api.commands.FindAndModifyCommand#Update.this"),
            isp("reactivemongo.api.commands.AggregationFramework#AggregationResult.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumValue.this"),
            isp("reactivemongo.api.commands.GroupAggregation#MinField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Avg.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumValue.this"),
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
            isp("reactivemongo.api.commands.AggregationFramework#Project.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Out.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupMulti.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.this"),
            isp("reactivemongo.api.commands.CreateUserCommand#CreateUser.this"),
            isp("reactivemongo.api.commands.AggregationFramework#MetadataSort.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFields.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Ascending.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Unwind.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPopField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#MetadataSort.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Last.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Redact.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Sum.this"),
            isp("reactivemongo.api.commands.GroupAggregation#First.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Match.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Max.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sample.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPopField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#First.this"),
            isp("reactivemongo.api.commands.AggregationFramework#AggregationResult.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GeoNear.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Ascending.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Min.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GraphLookup.this"),
            isp("reactivemongo.api.commands.GroupAggregation#Avg.this"),
            isp("reactivemongo.api.commands.GroupAggregation#FirstField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStatAccesses.this"),
            isp("reactivemongo.api.commands.GroupAggregation#StdDevPop.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Out.this"),
            isp("reactivemongo.api.commands.GroupAggregation#SumField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#IndexStats.this"),
            isp("reactivemongo.api.commands.AggregationFramework#GroupField.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Sort.this"),
            isp("reactivemongo.api.commands.GroupAggregation#PushField.this"),
            isp("reactivemongo.api.commands.GroupAggregation#AddFields.this"),
            isp("reactivemongo.api.commands.AggregationFramework#Filter.this"),
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
            isp("reactivemongo.api.commands.GroupAggregation#MaxField.this")
          )
        },
        Common.closeableObject in Test := "tests.Common$",
        testOptions in Test += Tests.Cleanup(Common.cleanup.value),
        mappings in (Compile, packageBin) ~= driverFilter,
        //mappings in (Compile, packageDoc) ~= driverFilter,
        mappings in (Compile, packageSrc) ~= driverFilter,
        apiMappings ++= Documentation.mappings("com.typesafe.akka", "http://doc.akka.io/api/akka/%s/")("akka-actor").value ++ Documentation.mappings("com.typesafe.play", "http://playframework.com/documentation/%s/api/scala/index.html", _.replaceAll("[\\d]$", "x"))("play-iteratees").value,
      Common.pomTransformer := {
        if (scalaBinaryVersion.value == "2.10") Some(skipScala210)
        else None
      }
    )).configure { p =>
      sys.props.get("test.nettyNativeArch") match {
        case Some("osx") => p.settings(Seq(
          libraryDependencies += shadedNative("osx-x86-64").value % Test
        ))

        case Some(_/* linux */) => p.settings(Seq(
          libraryDependencies += shadedNative("linux-x86-64").value % Test
        ))

        case _ => p
      }
    }.dependsOn(bson, core, bsonCompat, bsonmacros % Test)

  // ---

  private def shadedNative(arch: String) = Def.setting[ModuleID] {
    if (Common.useShaded.value) {
      val v = version.value
      val s = {
        if (v endsWith "-SNAPSHOT") {
          s"${v.dropRight(9)}-${arch}-SNAPSHOT"
        } else {
          s"${v}-${arch}"
        }
      }

      organization.value % "reactivemongo-shaded-native" % s
    } else {
      val variant = if (arch == "osx-x86-64") "kqueue" else "epoll"
      val classifier = if (arch == "osx-x86-64") "osx-x86_64" else "linux-x86_64"

      ("io.netty" % s"netty-transport-native-${variant}" % Dependencies.nettyVer).classifier(classifier)
    }
  }

  private val driverFilter: Seq[(File, String)] => Seq[(File, String)] = {
    (_: Seq[(File, String)]).filter {
      case (file, name) =>
        !(name endsWith "external/reactivemongo/StaticListenerBinder.class")
    }
  } andThen Common.filter

  private val driverCleanup = taskKey[Unit]("Driver compilation cleanup")

  // ---

  private lazy val skipScala210: XmlElem => Option[XmlElem] = { dep =>
    if ((dep \ "artifactId").text startsWith "reactivemongo-bson-compat") {
      None
    } else Some(dep)
  }
}
