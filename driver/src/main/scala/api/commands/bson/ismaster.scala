package reactivemongo.api.commands.bson

import reactivemongo.api.commands._

import reactivemongo.api.bson.collection.BSONSerializationPack

// TODO: Merge with api.commands.ismaster
private[reactivemongo] object BSONIsMasterCommand
  extends IsMasterCommand[BSONSerializationPack.type]
