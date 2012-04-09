package org.asyncmongo.protocol.messages

case class GetLastError(
  awaitJournalCommit: Boolean,
  waitForReplicatedOn: Option[Int],
  fsync: Boolean
)