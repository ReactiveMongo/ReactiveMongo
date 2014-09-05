package reactivemongo.api.commands

import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack }

trait ImplicitCommandHelpers[P <: SerializationPack] {
  trait ImplicitlyDocumentProducer {
    def produce: P#Document
  }
  object ImplicitlyDocumentProducer {
    implicit def producer[A](a: A)(implicit writer: P#Writer[A]): ImplicitlyDocumentProducer = ???
  }
}

trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Insert(
    documents: Stream[P#Document],
    ordered: Boolean) extends CollectionCommand

  object Insert {
    //def apply(documents: Stream[P#Document]): Insert = new Insert(documents)
    //def apply[A](a: A)(implicit writer: P#Writer[A]): Insert = Insert(Stream.empty) // todo
    def apply(docs: ImplicitlyDocumentProducer*): Insert = apply(true, docs: _*)
    def apply(ordered: Boolean, docs: ImplicitlyDocumentProducer*): Insert = Insert(docs.toStream.map(_.produce), ordered)
  }
}

object _insert_test {
  import reactivemongo.bson._

  object BSONInsertCommand extends InsertCommand[BSONSerializationPack.type]
  object BSONInsertCommand2 extends InsertCommand[BSONSerializationPack.type]

  import BSONInsertCommand._
  import BSONInsertCommand2.{ Insert => Insert2, _ }

  class Plop

  //type Coucou = Insert

  Insert(BSONDocument())
}