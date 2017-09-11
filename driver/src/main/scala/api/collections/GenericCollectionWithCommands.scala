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

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runner(self, command, readPreference)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runCommand[R, C](command, ReadPreference.primary)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runner.withResponse(self, command, readPreference)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runWithResponse[R, C](command, ReadPreference.primary)

  def runCommand[C <: CollectionCommand](command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = runner(self, command)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command, ReadPreference.primary)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command, rp)
}
