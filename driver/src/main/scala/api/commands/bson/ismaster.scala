package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

object BSONIsMasterCommand extends IsMasterCommand[BSONSerializationPack.type]

object BSONIsMasterCommandImplicits {
  import BSONIsMasterCommand._
  implicit object IsMasterWriter extends BSONDocumentWriter[IsMaster.type] {
    def write(im: IsMaster.type) =
      BSONDocument("ismaster" -> 1)
  }
  implicit object IsMasterResultReader extends BSONDocumentReader[IsMasterResult] {
    def read(doc: BSONDocument): IsMasterResult = {
      println(BSONDocument.pretty(doc))
      val rs = doc.getAs[String]("me").map { me =>
        ReplicaSet(
          setName = doc.getAs[String]("setName").get,
          me = me,
          primary = doc.getAs[String]("primary"),
          hosts = doc.getAs[Seq[String]]("hosts").getOrElse(Seq.empty),
          passives = doc.getAs[Seq[String]]("passives").getOrElse(Seq.empty),
          arbiters = doc.getAs[Seq[String]]("arbiters").getOrElse(Seq.empty),
          isSecondary = doc.getAs[Boolean]("secondary").getOrElse(false),
          isArbiterOnly = doc.getAs[Boolean]("arbiterOnly").getOrElse(false),
          isPassive = doc.getAs[Boolean]("passive").getOrElse(false),
          isHidden = doc.getAs[Boolean]("hidden").getOrElse(false),
          tags = doc.getAs[Seq[BSONDocument]]("tags").getOrElse(Seq.empty))
      }
      IsMasterResult(
        isMaster = doc.getAs[Boolean]("ismaster").getOrElse(false), // `ismaster`
        maxBsonObjectSize = doc.getAs[Int]("maxBsonObjectSize").getOrElse(16777216), // default = 16 * 1024 * 1024
        maxMessageSizeBytes = doc.getAs[Int]("maxMessageSizeBytes").getOrElse(48000000), // default = 48000000, mongod >= 2.4
        localTime = doc.getAs[BSONDateTime]("localTime").map(_.value), // date? mongod >= 2.2
        minWireVersion = doc.getAs[Int]("minWireVersion").getOrElse(0), // int? mongod >= 2.6
        maxWireVersion = doc.getAs[Int]("maxWireVersion").getOrElse(0), // int? mongod >= 2.6
        replicaSet = rs, // flattened in the result
        msg = doc.getAs[String]("msg") // Contains the value isdbgrid when isMaster returns from a mongos instance.
      )
    }

  }
}