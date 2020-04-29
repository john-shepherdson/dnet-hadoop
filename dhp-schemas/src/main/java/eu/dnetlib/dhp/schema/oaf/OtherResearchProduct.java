package eu.dnetlib.dhp.schema.oaf;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import java.io.Serializable;
import java.util.List;

public class OtherResearchProduct extends Result implements Serializable {

  private List<Field<String>> contactperson;

  private List<Field<String>> contactgroup;

  private List<Field<String>> tool;

  public OtherResearchProduct() {
    setResulttype(ModelConstants.ORP_DEFAULT_RESULTTYPE);
  }

  public List<Field<String>> getContactperson() {
    return contactperson;
  }

  public void setContactperson(List<Field<String>> contactperson) {
    this.contactperson = contactperson;
  }

  public List<Field<String>> getContactgroup() {
    return contactgroup;
  }

  public void setContactgroup(List<Field<String>> contactgroup) {
    this.contactgroup = contactgroup;
  }

  public List<Field<String>> getTool() {
    return tool;
  }

  public void setTool(List<Field<String>> tool) {
    this.tool = tool;
  }

  @Override
  public void mergeFrom(OafEntity e) {
    super.mergeFrom(e);

    if (!OtherResearchProduct.class.isAssignableFrom(e.getClass())) {
      return;
    }

    OtherResearchProduct o = (OtherResearchProduct) e;

    contactperson = mergeLists(contactperson, o.getContactperson());
    contactgroup = mergeLists(contactgroup, o.getContactgroup());
    tool = mergeLists(tool, o.getTool());
    mergeOAFDataInfo(e);
  }
}
