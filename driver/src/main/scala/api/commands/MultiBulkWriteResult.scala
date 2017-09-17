package reactivemongo.api.commands

object MultiBulkWriteResult {
  @deprecated("Use [[empty]]", "0.12.7")
  def apply(): MultiBulkWriteResult = empty

  def apply(wr: WriteResult): MultiBulkWriteResult = empty.merge(wr)

  def apply(rs: Iterable[WriteResult]): MultiBulkWriteResult =
    rs.foldLeft(empty)(_ merge _)

  val empty: MultiBulkWriteResult = MultiBulkWriteResult(
    true, 0, 0, Seq.empty, Seq.empty, None, None, None, 0)
}

case class MultiBulkWriteResult(
  ok: Boolean,
  n: Int,
  nModified: Int,
  upserted: Seq[Upserted],
  writeErrors: Seq[WriteError],
  writeConcernError: Option[WriteConcernError], // TODO ?
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
