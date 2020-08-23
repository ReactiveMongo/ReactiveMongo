package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ Cursor, ReadPreference, SerializationPack }

import reactivemongo.api.commands.{
  CollectionCommand,
  Command,
  CommandWithResult,
  CursorFetcher,
  ResponseResult,
  ResolvedCollectionCommand
}

/** Collection operations to run commands with */
private[api] trait GenericCollectionWithCommands[P <: SerializationPack] { self: GenericCollection[P] =>
  /** The command runner */
  def runner = Command.run(pack, self.failoverStrategy)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference = self.readPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runner(self, command, readPreference)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference = self.readPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runner.withResponse(self, command, readPreference)

  def runCommand[C <: CollectionCommand](command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = runner(self, command)
}
