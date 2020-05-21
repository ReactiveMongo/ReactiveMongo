package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

private[reactivemongo] trait MultiBulkWriteResultFactory[P <: SerializationPack] { self: PackSupport[P] with UpdateWriteResultFactory[P] with UpsertedFactory[P] =>
  /**
   * The result of a bulk write operation.
   *
   * @param ok the update status
   * @param n the number of documents selected for update
   * @param nModified the number of updated documents
   * @param upserted the upserted documents
   */
  final class MultiBulkWriteResult private (
    val ok: Boolean,
    val n: Int,
    val nModified: Int,
    val upserted: Seq[Upserted],
    val writeErrors: Seq[WriteError],
    val writeConcernError: Option[WriteConcernError],
    val code: Option[Int],
    val errmsg: Option[String],
    val totalN: Int) {

    @SuppressWarnings(Array("VariableShadowing"))
    private[api] def merge(wr: WriteResult): MultiBulkWriteResult = wr match {
      case wr: UpdateWriteResult @unchecked => new MultiBulkWriteResult(
        ok = ok && wr.ok,
        n = n + wr.n,
        writeErrors = writeErrors ++ wr.writeErrors.map(
          e => e.copy(index = e.index + totalN)),
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
