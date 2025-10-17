package eu.dnetlib.dhp.bulktag.resolver;

import java.io.Serializable;
import java.util.List;

public class RelatedEntity implements Serializable {
    private String entity;
    private String linkingResource;
    private String linkedResourceField;
    private String linkingAttribute;
    private String linkingAttributeValue;
    private String jsonPath;
    private String value;
    private List<String> attributes_to_select;
    private String joinOnLeft;
    private String joinOnRigth;

    public String getJoinOnLeft() {
        return joinOnLeft;
    }

    public void setJoinOnLeft(String joinOnLeft) {
        this.joinOnLeft = joinOnLeft;
    }

    public String getJoinOnRigth() {
        return joinOnRigth;
    }

    public void setJoinOnRigth(String joinOnRigth) {
        this.joinOnRigth = joinOnRigth;
    }

    public List<String> getAttributes_to_select() {
        return attributes_to_select;
    }

    public void setAttributes_to_select(List<String> attributes_to_select) {
        this.attributes_to_select = attributes_to_select;
    }

    public String getLinkingResource() {
        return linkingResource;
    }

    public void setLinkingResource(String linkingResource) {
        this.linkingResource = linkingResource;
    }

    public String getLinkedResourceField() {
        return linkedResourceField;
    }

    public void setLinkedResourceField(String linkedResourceField) {
        this.linkedResourceField = linkedResourceField;
    }

    public String getLinkingAttribute() {
        return linkingAttribute;
    }

    public void setLinkingAttribute(String linkingAttribute) {
        this.linkingAttribute = linkingAttribute;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }


    public String getLinkingAttributeValue() {
        return linkingAttributeValue;
    }

    public void setLinkingAttributeValue(String linkingAttributeValue) {
        this.linkingAttributeValue = linkingAttributeValue;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
