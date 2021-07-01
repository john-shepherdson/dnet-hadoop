package eu.dnetlib.dhp.sx.graph.scholix

import eu.dnetlib.dhp.schema.oaf.{Dataset, Result}
import eu.dnetlib.dhp.schema.sx.summary.{SchemeValue, ScholixSummary, TypedIdentifier, Typology}

import scala.collection.JavaConverters._

object ScholixUtils {


  def resultToSummary(r:Result):ScholixSummary = {
    val s = new ScholixSummary
    s.setId(r.getId)
    s.setLocalIdentifier(r.getPid.asScala.map(p => new TypedIdentifier(p.getValue, p.getQualifier.getClassid)).asJava)

    if (r.isInstanceOf[Dataset])
      s.setTypology(Typology.dataset)
    else
      s.setTypology(Typology.publication)

    s.setSubType(r.getInstance().get(0).getInstancetype.getClassname)

    if (r.getTitle!= null && r.getTitle.asScala.nonEmpty) {
      s.setTitle(r.getTitle.asScala.map(t => t.getValue).asJava)
    }

    if(r.getAuthor!= null && !r.getAuthor.isEmpty) {
      s.setAuthor(r.getAuthor.asScala.map(a=> a.getFullname).asJava)
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

    if (r.getSubject!= null && !r.getSubject.isEmpty)
      s.setSubject(r.getSubject.asScala.map(s => new SchemeValue(s.getQualifier.getClassname, s.getValue)).asJava)

    if (r.getPublisher!= null)
      s.setPublisher(List(r.getPublisher.getValue).asJava)

    s.setRelatedDatasets(0)
    s.setRelatedPublications(0)
    s.setRelatedUnknown(0)

    s
  }

}
