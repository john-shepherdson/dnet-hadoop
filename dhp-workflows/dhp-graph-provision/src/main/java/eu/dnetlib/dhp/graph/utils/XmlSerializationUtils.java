package eu.dnetlib.dhp.graph.utils;

import eu.dnetlib.dhp.schema.oaf.*;

import static eu.dnetlib.dhp.graph.utils.GraphMappingUtils.removePrefix;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class XmlSerializationUtils {

    // XML 1.0
    // #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
    private final static String xml10pattern = "[^"
            + "\u0009\r\n"
            + "\u0020-\uD7FF"
            + "\uE000-\uFFFD"
            + "\ud800\udc00-\udbff\udfff"
            + "]";

    public static String mapJournal(Journal j) {
        final String attrs = new StringBuilder()
            .append(attr("issn", j.getIssnPrinted()))
            .append(attr("eissn", j.getIssnOnline()))
            .append(attr("lissn", j.getIssnLinking()))
            .append(attr("ep", j.getEp()))
            .append(attr("iss", j.getIss()))
            .append(attr("sp", j.getSp()))
            .append(attr("vol", j.getVol()))
            .toString()
            .trim();

        return new StringBuilder()
            .append("<journal")
            .append(isNotBlank(attrs) ? (" " + attrs) : "")
            .append(">")
            .append(escapeXml(j.getName()))
            .append("</journal>")
            .toString();
    }

    private static String attr(final String name, final String value) {
        return isNotBlank(value) ? name + "=\"" + escapeXml(value) + "\" " : "";
    }

    public static String mapStructuredProperty(String name, StructuredProperty t) {
        return asXmlElement(name, t.getValue(), t.getQualifier(), t.getDataInfo() != null ? t.getDataInfo() : null);
    }

    public static String mapQualifier(String name, Qualifier q) {
        return asXmlElement(name, "", q, null);
    }

    public static String escapeXml(final String value) {
        return value
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll("\"", "&quot;")
                .replaceAll("'", "&apos;")
                .replaceAll(xml10pattern, "");
    }

    public static String parseDataInfo(final DataInfo dataInfo) {
        return new StringBuilder()
                .append("<datainfo>")
                .append(asXmlElement("inferred", dataInfo.getInferred() + ""))
                .append(asXmlElement("deletedbyinference", dataInfo.getDeletedbyinference() + ""))
                .append(asXmlElement("trust", dataInfo.getTrust() + ""))
                .append(asXmlElement("inferenceprovenance", dataInfo.getInferenceprovenance() + ""))
                .append(asXmlElement("provenanceaction", null, dataInfo.getProvenanceaction(), null))
                .append("</datainfo>")
                .toString();
    }

    private static StringBuilder dataInfoAsAttributes(final StringBuilder sb, final DataInfo info) {
        return sb
                .append(attr("inferred", info.getInferred() != null ? info.getInferred().toString() : ""))
                .append(attr("inferenceprovenance", info.getInferenceprovenance()))
                .append(attr("provenanceaction", info.getProvenanceaction() != null ? info.getProvenanceaction().getClassid() : ""))
                .append(attr("trust", info.getTrust()));
    }

    public static String mapKeyValue(final String name, final KeyValue kv) {
        return new StringBuilder()
                .append("<")
                .append(name)
                .append(" name=\"")
                .append(escapeXml(kv.getValue()))
                .append("\" id=\"")
                .append(escapeXml(removePrefix(kv.getKey())))
                .append("\"/>")
                .toString();
    }

    public static String mapExtraInfo(final ExtraInfo e) {
        return new StringBuilder("<extraInfo ")
                .append("name=\"" + e.getName() + "\" ")
                .append("typology=\"" + e.getTypology() + "\" ")
                .append("provenance=\"" + e.getProvenance() + "\" ")
                .append("trust=\"" + e.getTrust() + "\"")
                .append(">")
                .append(e.getValue())
                .append("</extraInfo>")
                .toString();
    }

    public static String asXmlElement(final String name, final String value) {
        return asXmlElement(name, value, null, null);
    }

    public static String asXmlElement(final String name, final String value, final Qualifier q, final DataInfo info) {
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(name);
        if (q != null) {
            sb.append(getAttributes(q));
        }
        if (info != null) {
            sb
                .append(" ")
                .append(attr("inferred", info.getInferred() != null ? info.getInferred().toString() : ""))
                .append(attr("inferenceprovenance", info.getInferenceprovenance()))
                .append(attr("provenanceaction", info.getProvenanceaction() != null ? info.getProvenanceaction().getClassid() : ""))
                .append(attr("trust", info.getTrust()));
        }
        if (isBlank(value)) {
            sb.append("/>");
            return sb.toString();
        }

        sb.append(">");
        sb.append(escapeXml(value));
        sb.append("</");
        sb.append(name);
        sb.append(">");

        return sb.toString();
    }

    public static String getAttributes(final Qualifier q) {
        if (q == null || q.isBlank()) return "";

        return new StringBuilder(" ")
                .append(attr("classid", q.getClassid()))
                .append(attr("classname", q.getClassname()))
                .append(attr("schemeid", q.getSchemeid()))
                .append(attr("schemename", q.getSchemename()))
                .toString();
    }

}
