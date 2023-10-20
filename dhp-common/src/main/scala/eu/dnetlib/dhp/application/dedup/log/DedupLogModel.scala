package eu.dnetlib.dhp.application.dedup.log

case class DedupLogModel(
  tag: String,
  configuration: String,
  entity: String,
  startTS: Long,
  endTS: Long,
  totalMs: Long
) {}
