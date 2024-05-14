
package eu.dnetlib.dhp.oa.provision.utils;

import static eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils.removePrefix;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class XmlSerializationUtils {

	// XML 1.0
	// #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
	private static final String XML_10_PATTERN = "[^" + "\u0009\r\n" + "\u0020-\uD7FF" + "\uE000-\uFFFD"
		+ "\ud800\udc00-\udbff\udfff" + "]";

	private XmlSerializationUtils() {
	}

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
		return asXmlElement(
			name, t.getValue(), t.getQualifier(), t.getDataInfo());
	}

	public static String mapQualifier(String name, Qualifier q) {
		return asXmlElement(name, "", q, null);
	}

	public static String escapeXml(final String value) {
		return value
			.replace("&", "&amp;")
			.replace("<", "&lt;")
			.replace(">", "&gt;")
			.replace("\"", "&quot;")
			.replace("'", "&apos;")
			.replaceAll(XML_10_PATTERN, "");
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

	public static String asXmlElement(
		final String name, final String value, final Qualifier q, final DataInfo info) {
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
				.append(
					attr(
						"provenanceaction",
						info.getProvenanceaction() != null
							? info.getProvenanceaction().getClassid()
							: ""))
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
		if (q == null || StringUtils.isBlank(q.getClassid()))
			return "";

		return new StringBuilder(" ")
			.append(attr("classid", q.getClassid()))
			.append(attr("classname", q.getClassname()))
			.append(attr("schemeid", q.getSchemeid()))
			.append(attr("schemename", q.getSchemename()))
			.toString();
	}

	public static String asXmlElement(String name, List<Tuple2<String, String>> attributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("<");
		sb.append(name);
		for (Tuple2<String, String> attr : attributes) {
			sb.append(" ").append(attr(attr._1(), attr._2()));
		}
		sb.append("/>");
		return sb.toString();
	}

	// <measure downloads="0" views="0">infrastruct_::f66f1bd369679b5b077dcdf006089556||OpenAIRE</measure>
	public static String usageMeasureAsXmlElement(String name, Measure measure) {
		HashSet<String> dsIds = Optional
			.ofNullable(measure.getUnit())
			.map(
				m -> m
					.stream()
					.map(KeyValue::getKey)
					.collect(Collectors.toCollection(HashSet::new)))
			.orElse(new HashSet<>());

		StringBuilder sb = new StringBuilder();
		dsIds.forEach(dsId -> {
			sb
				.append("<")
				.append(name);
			for (KeyValue kv : measure.getUnit()) {
				sb.append(" ").append(attr(measure.getId(), kv.getValue()));
			}
			sb
				.append(" ")
				.append(attr("datasource", dsId))
				.append("/>");
		});
		return sb.toString();
	}

	public static String mapEoscIf(EoscIfGuidelines e) {
		return asXmlElement(
			"eoscifguidelines", Lists
				.newArrayList(
					new Tuple2<>("code", e.getCode()),
					new Tuple2<>("label", e.getLabel()),
					new Tuple2<>("url", e.getUrl()),
					new Tuple2<>("semanticrelation", e.getSemanticRelation())));
	}

}
