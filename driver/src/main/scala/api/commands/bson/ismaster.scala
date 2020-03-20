package reactivemongo.api.commands.bson

import java.util.Date

import reactivemongo.core.ClientMetadata

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._

import reactivemongo.bson._ // TODO: Update

private[reactivemongo] object BSONIsMasterCommand
  extends IsMasterCommand[BSONSerializationPack.type]

private[reactivemongo] object BSONIsMasterCommandImplicits {
  import BSONIsMasterCommand._

  private val serializeClientMeta: ClientMetadata => Option[BSONDocument] =
    ClientMetadata.serialize[BSONSerializationPack.type](BSONSerializationPack)

  def IsMasterWriter[T <: IsMaster] = BSONDocumentWriter[T] { im: T =>
    val base = BSONDocument(
      "ismaster" -> 1,
      f"$$comment" -> im.comment)

    im.client.fold(base) { meta =>
      base ++ ("client" -> serializeClientMeta(meta))
    }
  }

  implicit object IsMasterResultReader extends DealingWithGenericCommandErrorsReader[IsMasterResult] {
    def readResult(doc: BSONDocument): IsMasterResult = {
      def rs = doc.getAs[String]("me").map { me =>
        new ReplicaSet(
          setName = doc.getAs[String]("setName").get,
          setVersion = doc.getAs[BSONNumberLike]("setVersion").
            fold(-1)(_.toInt),
          me = me,
          primary = doc.getAs[String]("primary"),
          hosts = doc.getAs[Seq[String]]("hosts").getOrElse(Seq.empty),
          passives = doc.getAs[Seq[String]]("passives").getOrElse(Seq.empty),
          arbiters = doc.getAs[Seq[String]]("arbiters").getOrElse(Seq.empty),
          isSecondary = doc.getAs[BSONBooleanLike](
            "secondary").fold(false)(_.toBoolean),
          isArbiterOnly = doc.getAs[BSONBooleanLike](
            "arbiterOnly").fold(false)(_.toBoolean),
          isPassive = doc.getAs[BSONBooleanLike](
            "passive").fold(false)(_.toBoolean),
          isHidden = doc.getAs[BSONBooleanLike]("hidden").
            fold(false)(_.toBoolean),
          tags = doc.getAs[BSONDocument]("tags").map {
            _.elements.collect {
              case BSONElement(tag, BSONString(v)) => tag -> v
            }.toMap
          }.getOrElse(Map.empty),
          electionId = doc.getAs[BSONNumberLike]("electionId").
            fold(-1)(_.toInt),
          lastWrite = doc.getAs[BSONDocument]("lastWrite").flatMap { ld =>
            for {
              opTime <- ld.getAs[BSONNumberLike]("opTime")
              lastWriteDate <- ld.getAs[Date]("lastWriteDate")
              majorityOpTime <- ld.getAs[BSONNumberLike]("majorityOpTime")
              majorityWriteDate <- ld.getAs[Date]("majorityWriteDate")
            } yield new LastWrite(
              opTime.toLong, lastWriteDate,
              majorityOpTime.toLong, majorityWriteDate)
          })

      }

      new IsMasterResult(
        isMaster = doc.getAs[BSONBooleanLike](
          "ismaster").fold(false)(_.toBoolean), // `ismaster`
        maxBsonObjectSize = doc.getAs[BSONNumberLike]("maxBsonObjectSize").
          fold[Int](16777216)(_.toInt), // default = 16 * 1024 * 1024
        maxMessageSizeBytes = doc.getAs[BSONNumberLike]("maxMessageSizeBytes").
          fold[Int](48000000)(_.toInt), // default = 48000000, mongod >= 2.4
        maxWriteBatchSize = doc.getAs[BSONNumberLike]("maxWriteBatchSize").
          fold[Int](1000)(_.toInt),
        localTime = doc.getAs[BSONDateTime]("localTime").map(_.value), // date? mongod >= 2.2
        logicalSessionTimeoutMinutes = doc.getAs[BSONNumberLike]("logicalSessionTimeoutMinutes").map(_.toLong),
        minWireVersion = doc.getAs[BSONNumberLike]("minWireVersion").
          fold[Int](0)(_.toInt), // int? mongod >= 2.6
        maxWireVersion = doc.getAs[BSONNumberLike]("maxWireVersion").
          fold[Int](0)(_.toInt), // int? mongod >= 2.6
        readOnly = doc.getAs[BSONBooleanLike]("readOnly").map(_.toBoolean),
        compression = doc.getAs[List[String]]("compression").getOrElse(List.empty),
        saslSupportedMech = doc.getAs[List[String]]("saslSupportedMech").getOrElse(List.empty),
        replicaSet = rs, // flattened in the result
        msg = doc.getAs[String]("msg") // Contains the value isdbgrid when isMaster returns from a mongos instance.
      )
    }
  }
}
