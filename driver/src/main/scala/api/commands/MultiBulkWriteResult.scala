package reactivemongo.api.commands

// TODO#1.1: Move to `api` ?
@deprecated("Will be replaced by `reactivemongo.api.MultiWriteResult`", "0.19.8")
case class MultiBulkWriteResult(
  ok: Boolean,
  n: Int,
  nModified: Int,
  upserted: Seq[Upserted],
  writeErrors: Seq[WriteError],
  writeConcernError: Option[WriteConcernError],
  code: Option[Int],
  errmsg: Option[String],
  totalN: Int) {
  def merge(wr: WriteResult): MultiBulkWriteResult = wr match {
    case wr: UpdateWriteResult => MultiBulkWriteResult(
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
      MultiBulkWriteResult(
        ok = ok && wr.ok,
        n = n + wr.n,
        writeErrors = writeErrors ++ wr.writeErrors.map(e => e.copy(index = e.index + totalN)),
        writeConcernError = writeConcernError.orElse(wr.writeConcernError),
        code = code.orElse(wr.code),
        errmsg = errmsg.orElse(wr.errmsg),
        nModified = nModified,
        upserted = upserted,
        totalN = totalN + wr.n + wr.writeErrors.size)
  }
}

@deprecated("Will be replaced by `reactivemongo.api.MultiWriteResult`", "0.19.8")
object MultiBulkWriteResult {
  def apply(wr: WriteResult): MultiBulkWriteResult = empty.merge(wr)

  def apply(rs: Iterable[WriteResult]): MultiBulkWriteResult =
    rs.foldLeft(empty)(_ merge _)

  val empty: MultiBulkWriteResult = MultiBulkWriteResult(
    true, 0, 0, Seq.empty, Seq.empty, None, None, None, 0)
}
