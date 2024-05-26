package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ FailoverStrategy, Cursor, ReadPreference, SerializationPack }
import reactivemongo.api.commands.{
  CollectionCommand,
  Command,
  CommandWithResult,
  CursorFetcher,
  ResolvedCollectionCommand,
  ResponseResult
}

/** Collection operations to run commands with */
private[api] trait GenericCollectionWithCommands[P <: SerializationPack] {
  //self: GenericCollection[P] =>

  // Workaround for self-type issue with Scala 3
  // https://github.com/scala/scala3/issues/11226
  protected def selfCollection: GenericCollection[P]

  val pack: P

  def readPreference: ReadPreference

  def failoverStrategy: FailoverStrategy

  /** The command runner */
  def runner = Command.run(pack, this.failoverStrategy)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](
      command: C with CommandWithResult[R],
      readPreference: ReadPreference = this.readPreference
    )(implicit
      writer: pack.Writer[ResolvedCollectionCommand[C]],
      reader: pack.Reader[R],
      ec: ExecutionContext
    ): Future[R] = runner(selfCollection, command, readPreference)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](
      command: C with CommandWithResult[R],
      readPreference: ReadPreference = this.readPreference
    )(implicit
      writer: pack.Writer[ResolvedCollectionCommand[C]],
      reader: pack.Reader[R],
      ec: ExecutionContext
    ): Future[ResponseResult[R]] =
    runner.withResponse(selfCollection, command, readPreference)

  def runCommand[C <: CollectionCommand](
      command: C
    )(implicit
      writer: pack.Writer[ResolvedCollectionCommand[C]]
    ): CursorFetcher[pack.type, Cursor] = runner(selfCollection, command)
}
