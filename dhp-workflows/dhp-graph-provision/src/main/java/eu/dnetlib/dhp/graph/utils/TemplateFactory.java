package eu.dnetlib.dhp.graph.utils;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.graph.utils.GraphMappingUtils.removePrefix;
import static eu.dnetlib.dhp.graph.utils.XmlSerializationUtils.escapeXml;

public class TemplateFactory {

    private TemplateResources resources;

    private final static char DELIMITER = '$';

    public TemplateFactory() {
        try {
            resources = new TemplateResources();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String buildBody(final String type, final List<String> metadata, final List<String> rels, final List<String> children, final List<String> extraInfo) {
        ST body = getTemplate(resources.getEntity());

        body.add("name", type);
        body.add("metadata", metadata);
        body.add("rels", rels);
        body.add("children", children);
        body.add("extrainfo", extraInfo);

        return body.render();
    }

    public String getChild(final String name, final String id, final List<String> metadata) {
        return getTemplate(resources.getChild())
            .add("name", name)
            .add("hasId", !(id == null))
            .add("id", id != null ? escapeXml(removePrefix(id)) : "")
            .add("metadata", metadata)
            .render();
    }

    public String buildRecord(
            final OafEntity entity,
            final String schemaLocation,
            final String body) {
        return getTemplate(resources.getRecord())
            .add("id", escapeXml(removePrefix(entity.getId())))
            .add("dateofcollection", entity.getDateofcollection())
            .add("dateoftransformation", entity.getDateoftransformation())
            .add("schemaLocation", schemaLocation)
            .add("it", body)
            .render();
    }

    public String getRel(final String type,
                         final String objIdentifier,
                         final Collection<String> fields,
                         final String semanticclass,
                         final String semantischeme,
                         final DataInfo info) {
        return getTemplate(resources.getRel())
            .add("type", type)
            .add("objIdentifier", escapeXml(removePrefix(objIdentifier)))
            .add("class", semanticclass)
            .add("scheme", semantischeme)
            .add("metadata", fields)
            .add("inferred", info.getInferred())
            .add("trust", info.getTrust())
            .add("inferenceprovenance", info.getInferenceprovenance())
            .add("provenanceaction", info.getProvenanceaction() != null ? info.getProvenanceaction().getClassid() : "")
            .render();
    }

    public String getInstance(final String resultId, final List<String> instancemetadata, final List<String> webresources) {
        return getTemplate(resources.getInstance())
            .add("instanceId", escapeXml(removePrefix(resultId)))
            .add("metadata", instancemetadata)
            .add("webresources", webresources
                .stream()
                .filter(StringUtils::isNotBlank)
                .map(w -> getWebResource(w))
                .collect(Collectors.toList()))
            .render();
    }

    private String getWebResource(final String identifier) {
        return getTemplate(resources.getWebresource())
            .add("identifier", escapeXml(identifier))
            .render();
    }

    // HELPERS

    private ST getTemplate(final String res) {
        return new ST(res, DELIMITER, DELIMITER);
    }

}
