package eu.dnetlib.dhp.provision.scholix.summary;

import java.io.Serializable;

public class TypedIdentifier implements Serializable {
  private String id;
  private String type;

  public TypedIdentifier() {}

  public TypedIdentifier(String id, String type) {
    this.id = id;
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
