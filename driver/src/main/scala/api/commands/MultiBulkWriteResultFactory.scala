package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

trait MultiBulkWriteResultFactory[P <: SerializationPack] { self: PackSupport[P] with UpdateWriteResultFactory[P] with UpsertedFactory[P] =>
  /**
   * The result of a bulk write operation.
   */
  final class MultiBulkWriteResult private (
    _ok: Boolean,
    _n: Int,
    _nModified: Int,
    _upserted: Seq[Upserted],
    _writeErrors: Seq[WriteError],
    _writeConcernError: Option[WriteConcernError],
    _code: Option[Int],
    _errmsg: Option[String],
    _totalN: Int) {

    /** The update status */
    @inline def ok = _ok

    /** The number of documents selected for update */
    @inline def n = _n

    /** The number of updated documents */
    @inline def nModified = _nModified

    /** The upserted documents */
    @inline def upserted = _upserted

    @inline def writeErrors = _writeErrors
    @inline def writeConcernError = _writeConcernError

    /** The result code */
    @inline def code = _code

    /** The message if the result is erroneous */
    @inline def errmsg = _errmsg

    @inline def totalN = _totalN

    @SuppressWarnings(Array("VariableShadowing"))
    private[api] def merge(wr: WriteResult): MultiBulkWriteResult = wr match {
      case wr: UpdateWriteResult @unchecked => new MultiBulkWriteResult(
        _ok = ok && wr.ok,
        _n = n + wr.n,
        _writeErrors = writeErrors ++ wr.writeErrors.map(
          e => e.copy(index = e.index + totalN)),
        _writeConcernError = writeConcernError.orElse(wr.writeConcernError),
        _code = code.orElse(wr.code),
        _errmsg = errmsg.orElse(wr.errmsg),
        _nModified = wr.nModified,
        _upserted = wr.upserted,
        _totalN = totalN + wr.n + wr.writeErrors.size)

      case _ =>
        new MultiBulkWriteResult(
          _ok = ok && wr.ok,
          _n = n + wr.n,
          _writeErrors = writeErrors ++ wr.writeErrors.map(e =>
            e.copy(index = e.index + totalN)),
          _writeConcernError = writeConcernError.orElse(wr.writeConcernError),
          _code = code.orElse(wr.code),
          _errmsg = errmsg.orElse(wr.errmsg),
          _nModified = nModified,
          _upserted = upserted,
          _totalN = totalN + wr.n + wr.writeErrors.size)
    }

    private lazy val tupled = Tuple9(ok, n, nModified, upserted, writeErrors, writeConcernError, code, errmsg, totalN)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        other.tupled == this.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"MultiBulkWriteResult${tupled.toString}"
  }

  private[api] object MultiBulkWriteResult {
    def apply(rs: Iterable[WriteResult]): MultiBulkWriteResult =
      rs.foldLeft(empty)(_ merge _)

    val empty: MultiBulkWriteResult = new MultiBulkWriteResult(
      true, 0, 0, Seq.empty, Seq.empty, None, None, None, 0)
  }
}
