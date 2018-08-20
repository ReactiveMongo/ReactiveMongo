package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ Cursor, ReadPreference, SerializationPack }

import reactivemongo.api.commands.{
  BoxedAnyVal,
  CollectionCommand,
  Command,
  CommandWithResult,
  CursorFetcher,
  ResponseResult,
  ResolvedCollectionCommand
}

/** Collection operations to run commands with */
trait GenericCollectionWithCommands[P <: SerializationPack with Singleton] { self: GenericCollection[P] =>
  val pack: P

  def runner = Command.run(pack, self.failoverStrategy)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference = self.readPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runner(self, command, readPreference)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference = self.readPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runner.withResponse(self, command, readPreference)

  def runCommand[C <: CollectionCommand](command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = runner(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference = self.readPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command, rp)
}
