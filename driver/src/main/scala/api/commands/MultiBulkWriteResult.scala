package reactivemongo.api.commands

final class MultiBulkWriteResult private[api] (
  val ok: Boolean,
  val n: Int,
  val nModified: Int,
  val upserted: Seq[Upserted],
  val writeErrors: Seq[WriteError],
  val writeConcernError: Option[WriteConcernError],
  val code: Option[Int],
  val errmsg: Option[String],
  val totalN: Int) {

  private[api] def merge(wr: WriteResult): MultiBulkWriteResult = wr match {
    case wr: UpdateWriteResult => new MultiBulkWriteResult(
      ok = ok && wr.ok,
      n = n + wr.n,
      writeErrors = writeErrors ++ wr.writeErrors.map(e => e.copy(index = e.index + totalN)),
      writeConcernError = writeConcernError.orElse(wr.writeConcernError),
      code = code.orElse(wr.code),
      errmsg = errmsg.orElse(wr.errmsg),
      nModified = wr.nModified,
      upserted = wr.upserted,
      totalN = totalN + wr.n + wr.writeErrors.size)

    case _ =>
      new MultiBulkWriteResult(
        ok = ok && wr.ok,
        n = n + wr.n,
        writeErrors = writeErrors ++ wr.writeErrors.map(e =>
          e.copy(index = e.index + totalN)),
        writeConcernError = writeConcernError.orElse(wr.writeConcernError),
        code = code.orElse(wr.code),
        errmsg = errmsg.orElse(wr.errmsg),
        nModified = nModified,
        upserted = upserted,
        totalN = totalN + wr.n + wr.writeErrors.size)
  }

  private lazy val tupled = Tuple9(ok, n, nModified, upserted, writeErrors, writeConcernError, code, errmsg, totalN)

  override def equals(that: Any): Boolean = that match {
    case other: MultiBulkWriteResult =>
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
