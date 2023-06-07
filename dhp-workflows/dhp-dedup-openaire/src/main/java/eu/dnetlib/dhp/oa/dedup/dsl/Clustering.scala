package eu.dnetlib.dhp.oa.dedup.dsl

case class Clustering(name: String = "",
                 fields: Seq[String] = Seq(),
                 params: Map[String,Int] = Map()) {
  def withName(name: String) : Clustering =
    copy(name = name)

  def withFields(fields: String*): Clustering =
    copy(fields = fields)

  def withParams(params: Map[String,Int]): Clustering =
    copy(params = params)
}

