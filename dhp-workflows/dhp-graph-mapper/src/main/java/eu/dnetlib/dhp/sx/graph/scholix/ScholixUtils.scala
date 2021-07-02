package eu.dnetlib.dhp.sx.graph.scholix

import eu.dnetlib.dhp.schema.oaf.{Dataset, Result}
import eu.dnetlib.dhp.schema.sx.summary.{CollectedFromType, SchemeValue, ScholixSummary, TypedIdentifier, Typology}

import scala.collection.JavaConverters._
import scala.language.postfixOps

object ScholixUtils {


  def resultToSummary(r:Result):ScholixSummary = {
    val s = new ScholixSummary
    s.setId(r.getId)
    if (r.getPid == null || r.getPid.isEmpty)
      return null

    val pids:List[TypedIdentifier] = r.getPid.asScala.map(p => new TypedIdentifier(p.getValue, p.getQualifier.getClassid))(collection breakOut)
    s.setLocalIdentifier(pids.asJava)

    s.getLocalIdentifier.isEmpty

    if (r.isInstanceOf[Dataset])
      s.setTypology(Typology.dataset)
    else
      s.setTypology(Typology.publication)

    s.setSubType(r.getInstance().get(0).getInstancetype.getClassname)

    if (r.getTitle!= null && r.getTitle.asScala.nonEmpty) {
      val titles:List[String] =r.getTitle.asScala.map(t => t.getValue)(collection breakOut)
      if (titles.nonEmpty)
        s.setTitle(titles.asJava)
      else
        return  null
    }

    if(r.getAuthor!= null && !r.getAuthor.isEmpty) {
      val authors:List[String] = r.getAuthor.asScala.map(a=> a.getFullname)(collection breakOut)
      if (authors nonEmpty)
        s.setAuthor(authors.asJava)
    }
    if (r.getInstance() != null) {
      val dt:List[String] = r.getInstance().asScala.filter(i => i.getDateofacceptance != null).map(i => i.getDateofacceptance.getValue)(collection.breakOut)
      if (dt.nonEmpty)
        s.setDate(dt.asJava)
    }
    if (r.getDescription!= null && !r.getDescription.isEmpty) {
      val d = r.getDescription.asScala.find(f => f.getValue!=null)
      if (d.isDefined)
        s.setDescription(d.get.getValue)
    }

    if (r.getSubject!= null && !r.getSubject.isEmpty) {
      val subjects:List[SchemeValue] =r.getSubject.asScala.map(s => new SchemeValue(s.getQualifier.getClassname, s.getValue))(collection breakOut)
      if (subjects.nonEmpty)
        s.setSubject(subjects.asJava)
    }

    if (r.getPublisher!= null)
      s.setPublisher(List(r.getPublisher.getValue).asJava)

    if (r.getCollectedfrom!= null && !r.getCollectedfrom.isEmpty) {
      val cf:List[CollectedFromType] = r.getCollectedfrom.asScala.map(c => new CollectedFromType(c.getValue, c.getKey, "complete"))(collection breakOut)
      if (cf.nonEmpty)
        s.setDatasources(cf.asJava)
    }

    s.setRelatedDatasets(0)
    s.setRelatedPublications(0)
    s.setRelatedUnknown(0)

    s
  }

}
