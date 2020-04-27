package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;

public class ScholixIdentifier implements Serializable {
  private String identifier;
  private String schema;

  public ScholixIdentifier() {}

  public ScholixIdentifier(String identifier, String schema) {
    this.identifier = identifier;
    this.schema = schema;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }
}
