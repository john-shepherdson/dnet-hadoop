
package eu.dnetlib.dhp.oa.provision.utils;

import static eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils.*;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;

import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.MainEntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.Result;

public class XmlRecordFactory implements Serializable {

	private static final String REL_SUBTYPE_DEDUP = "dedup";
	private final Map<String, LongAccumulator> accumulators;

	private final Set<String> specialDatasourceTypes;

	private final ContextMapper contextMapper;

	private final String schemaLocation;

	private boolean indent = false;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public XmlRecordFactory(
		final ContextMapper contextMapper,
		final boolean indent,
		final String schemaLocation,
		final String otherDatasourceTypesUForUI) {

		this(Maps.newHashMap(), contextMapper, indent, schemaLocation, otherDatasourceTypesUForUI);
	}

	public XmlRecordFactory(
		final Map<String, LongAccumulator> accumulators,
		final ContextMapper contextMapper,
		final boolean indent,
		final String schemaLocation,
		final String otherDatasourceTypesUForUI) {

		this.accumulators = accumulators;
		this.contextMapper = contextMapper;
		this.schemaLocation = schemaLocation;
		this.specialDatasourceTypes = Sets.newHashSet(Splitter.on(",").trimResults().split(otherDatasourceTypesUForUI));

		this.indent = indent;
	}

	public String build(final JoinedEntity je) {

		final Set<String> contexts = Sets.newHashSet();

		// final OafEntity entity = toOafEntity(je.getEntity());
		OafEntity entity = je.getEntity();
		TemplateFactory templateFactory = new TemplateFactory();
		try {

			final EntityType type = EntityType.fromClass(entity.getClass());
			final List<String> metadata = metadata(type, entity, contexts);

			// rels has to be processed before the contexts because they enrich the contextMap with
			// the
			// funding info.
			final List<RelatedEntityWrapper> links = je.getLinks();
			final List<String> relations = links
				.stream()
				.filter(link -> !isDuplicate(link))
				.map(link -> mapRelation(contexts, templateFactory, type, link))
				.collect(Collectors.toCollection(ArrayList::new));

			final String mainType = ModelSupport.getMainType(type);
			metadata.addAll(buildContexts(mainType, contexts));
			metadata.add(XmlSerializationUtils.parseDataInfo(entity.getDataInfo()));

			final String body = templateFactory
				.buildBody(
					mainType,
					metadata,
					relations,
					listChildren(entity, je, templateFactory),
					listExtraInfo(entity));

			return printXML(templateFactory.buildRecord(entity, schemaLocation, body), indent);
		} catch (final Throwable e) {
			throw new RuntimeException(String.format("error building record '%s'", entity.getId()), e);
		}
	}

	private static OafEntity toOafEntity(TypedRow typedRow) {
		return parseOaf(typedRow.getOaf(), typedRow.getType());
	}

	private static OafEntity parseOaf(final String json, final String type) {
		try {
			switch (EntityType.valueOf(type)) {
				case publication:
					return OBJECT_MAPPER.readValue(json, Publication.class);
				case dataset:
					return OBJECT_MAPPER.readValue(json, Dataset.class);
				case otherresearchproduct:
					return OBJECT_MAPPER.readValue(json, OtherResearchProduct.class);
				case software:
					return OBJECT_MAPPER.readValue(json, Software.class);
				case datasource:
					return OBJECT_MAPPER.readValue(json, Datasource.class);
				case organization:
					return OBJECT_MAPPER.readValue(json, Organization.class);
				case project:
					return OBJECT_MAPPER.readValue(json, Project.class);
				default:
					throw new IllegalArgumentException("invalid type: " + type);
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private String printXML(String xml, boolean indent) {
		try {
			final Document doc = new SAXReader().read(new StringReader(xml));
			OutputFormat format = indent ? OutputFormat.createPrettyPrint() : OutputFormat.createCompactFormat();
			format.setExpandEmptyElements(false);
			format.setSuppressDeclaration(true);
			StringWriter sw = new StringWriter();
			XMLWriter writer = new XMLWriter(sw, format);
			writer.write(doc);
			return sw.toString();
		} catch (IOException | DocumentException e) {
			throw new IllegalArgumentException("Unable to indent XML. Invalid record:\n" + xml, e);
		}
	}

	private List<String> metadata(
		final EntityType type, final OafEntity entity, final Set<String> contexts) {

		final List<String> metadata = Lists.newArrayList();

		if (entity.getCollectedfrom() != null) {
			metadata
				.addAll(
					entity
						.getCollectedfrom()
						.stream()
						.filter(XmlRecordFactory::kvNotBlank)
						.map(kv -> XmlSerializationUtils.mapKeyValue("collectedfrom", kv))
						.collect(Collectors.toList()));
		}
		if (entity.getOriginalId() != null) {
			metadata
				.addAll(
					entity
						.getOriginalId()
						.stream()
						.filter(Objects::nonNull)
						.map(s -> XmlSerializationUtils.asXmlElement("originalId", s))
						.collect(Collectors.toList()));
		}
		if (entity.getPid() != null) {
			metadata
				.addAll(
					entity
						.getPid()
						.stream()
						.filter(Objects::nonNull)
						.map(p -> XmlSerializationUtils.mapStructuredProperty("pid", p))
						.collect(Collectors.toList()));
		}

		if (ModelSupport.isResult(type)) {
			final Result r = (Result) entity;

			if (r.getContext() != null) {
				contexts.addAll(r.getContext().stream().map(c -> c.getId()).collect(Collectors.toList()));
				/* FIXME: Workaround for CLARIN mining issue: #3670#note-29 */
				if (contexts.contains("dh-ch::subcommunity::2")) {
					contexts.add("clarin");
				}
			}

			if (r.getTitle() != null) {
				metadata
					.addAll(
						r
							.getTitle()
							.stream()
							.filter(Objects::nonNull)
							.map(t -> XmlSerializationUtils.mapStructuredProperty("title", t))
							.collect(Collectors.toList()));
			}
			if (r.getBestaccessright() != null) {
				metadata.add(XmlSerializationUtils.mapQualifier("bestaccessright", r.getBestaccessright()));
			}
			if (r.getAuthor() != null) {
				metadata
					.addAll(
						r
							.getAuthor()
							.stream()
							.filter(Objects::nonNull)
							.map(
								a -> {
									final StringBuilder sb = new StringBuilder("<creator rank=\"" + a.getRank() + "\"");
									if (isNotBlank(a.getName())) {
										sb.append(" name=\"" + XmlSerializationUtils.escapeXml(a.getName()) + "\"");
									}
									if (isNotBlank(a.getSurname())) {
										sb
											.append(
												" surname=\"" + XmlSerializationUtils.escapeXml(a.getSurname()) + "\"");
									}
									if (a.getPid() != null) {
										a
											.getPid()
											.stream()
											.filter(Objects::nonNull)
											.filter(
												sp -> isNotBlank(sp.getQualifier().getClassid())
													&& isNotBlank(sp.getValue()))
											.collect(
												Collectors
													.toMap(
														p -> getAuthorPidType(p.getQualifier().getClassid()),
														p -> p,
														(p1, p2) -> p1))
											.values()
											.forEach(
												sp -> {
													String pidType = getAuthorPidType(sp.getQualifier().getClassid());
													String pidValue = XmlSerializationUtils.escapeXml(sp.getValue());

													// ugly hack: some records provide swapped pidtype and pidvalue
													if (authorPidTypes.contains(pidValue.toLowerCase().trim())) {
														sb.append(String.format(" %s=\"%s\"", pidValue, pidType));
													} else {
														if (isNotBlank(pidType)) {
															sb
																.append(
																	String
																		.format(
																			" %s=\"%s\"",
																			pidType,
																			pidValue
																				.toLowerCase()
																				.replaceAll("orcid", "")));
														}
													}
												});
									}
									sb
										.append(
											">" + XmlSerializationUtils.escapeXml(a.getFullname()) + "</creator>");
									return sb.toString();
								})
							.collect(Collectors.toList()));
			}
			if (r.getContributor() != null) {
				metadata
					.addAll(
						r
							.getContributor()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.asXmlElement("contributor", c.getValue()))
							.collect(Collectors.toList()));
			}
			if (r.getCountry() != null) {
				metadata
					.addAll(
						r
							.getCountry()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.mapQualifier("country", c))
							.collect(Collectors.toList()));
			}
			if (r.getCoverage() != null) {
				metadata
					.addAll(
						r
							.getCoverage()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.asXmlElement("coverage", c.getValue()))
							.collect(Collectors.toList()));
			}
			if (r.getDateofacceptance() != null) {
				metadata
					.add(
						XmlSerializationUtils
							.asXmlElement(
								"dateofacceptance", r.getDateofacceptance().getValue()));
			}
			if (r.getDescription() != null) {
				metadata
					.addAll(
						r
							.getDescription()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.asXmlElement("description", c.getValue()))
							.collect(Collectors.toList()));
			}
			if (r.getEmbargoenddate() != null) {
				metadata
					.add(
						XmlSerializationUtils.asXmlElement("embargoenddate", r.getEmbargoenddate().getValue()));
			}
			if (r.getSubject() != null) {
				metadata
					.addAll(
						r
							.getSubject()
							.stream()
							.filter(Objects::nonNull)
							.map(s -> XmlSerializationUtils.mapStructuredProperty("subject", s))
							.collect(Collectors.toList()));
			}
			if (r.getLanguage() != null) {
				metadata.add(XmlSerializationUtils.mapQualifier("language", r.getLanguage()));
			}
			if (r.getRelevantdate() != null) {
				metadata
					.addAll(
						r
							.getRelevantdate()
							.stream()
							.filter(Objects::nonNull)
							.map(s -> XmlSerializationUtils.mapStructuredProperty("relevantdate", s))
							.collect(Collectors.toList()));
			}
			if (r.getPublisher() != null) {
				metadata.add(XmlSerializationUtils.asXmlElement("publisher", r.getPublisher().getValue()));
			}
			if (r.getSource() != null) {
				metadata
					.addAll(
						r
							.getSource()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.asXmlElement("source", c.getValue()))
							.collect(Collectors.toList()));
			}
			if (r.getFormat() != null) {
				metadata
					.addAll(
						r
							.getFormat()
							.stream()
							.filter(Objects::nonNull)
							.map(c -> XmlSerializationUtils.asXmlElement("format", c.getValue()))
							.collect(Collectors.toList()));
			}
			if (r.getResulttype() != null) {
				metadata.add(XmlSerializationUtils.mapQualifier("resulttype", r.getResulttype()));
			}
			if (r.getResourcetype() != null) {
				metadata.add(XmlSerializationUtils.mapQualifier("resourcetype", r.getResourcetype()));
			}
		}

		switch (type) {
			case publication:
				final Publication pub = (Publication) entity;

				if (pub.getJournal() != null) {
					final Journal j = pub.getJournal();
					metadata.add(XmlSerializationUtils.mapJournal(j));
				}

				break;
			case dataset:
				final Dataset d = (Dataset) entity;
				if (d.getDevice() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("device", d.getDevice().getValue()));
				}
				if (d.getLastmetadataupdate() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"lastmetadataupdate", d.getLastmetadataupdate().getValue()));
				}
				if (d.getMetadataversionnumber() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"metadataversionnumber", d.getMetadataversionnumber().getValue()));
				}
				if (d.getSize() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("size", d.getSize().getValue()));
				}
				if (d.getStoragedate() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("storagedate", d.getStoragedate().getValue()));
				}
				if (d.getVersion() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("version", d.getVersion().getValue()));
				}
				// TODO d.getGeolocation()

				break;
			case otherresearchproduct:
				final OtherResearchProduct orp = (OtherResearchProduct) entity;

				if (orp.getContactperson() != null) {
					metadata
						.addAll(
							orp
								.getContactperson()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("contactperson", c.getValue()))
								.collect(Collectors.toList()));
				}

				if (orp.getContactgroup() != null) {
					metadata
						.addAll(
							orp
								.getContactgroup()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("contactgroup", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (orp.getTool() != null) {
					metadata
						.addAll(
							orp
								.getTool()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("tool", c.getValue()))
								.collect(Collectors.toList()));
				}
				break;
			case software:
				final Software s = (Software) entity;

				if (s.getDocumentationUrl() != null) {
					metadata
						.addAll(
							s
								.getDocumentationUrl()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("documentationUrl", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (s.getLicense() != null) {
					metadata
						.addAll(
							s
								.getLicense()
								.stream()
								.filter(Objects::nonNull)
								.map(l -> XmlSerializationUtils.mapStructuredProperty("license", l))
								.collect(Collectors.toList()));
				}
				if (s.getCodeRepositoryUrl() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"codeRepositoryUrl", s.getCodeRepositoryUrl().getValue()));
				}
				if (s.getProgrammingLanguage() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.mapQualifier(
									"programmingLanguage", s.getProgrammingLanguage()));
				}
				break;
			case datasource:
				final Datasource ds = (Datasource) entity;

				if (ds.getDatasourcetype() != null) {
					mapDatasourceType(metadata, ds.getDatasourcetype());
				}
				if (ds.getOpenairecompatibility() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.mapQualifier(
									"openairecompatibility", ds.getOpenairecompatibility()));
				}
				if (ds.getOfficialname() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("officialname", ds.getOfficialname().getValue()));
				}
				if (ds.getEnglishname() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("englishname", ds.getEnglishname().getValue()));
				}
				if (ds.getWebsiteurl() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("websiteurl", ds.getWebsiteurl().getValue()));
				}
				if (ds.getLogourl() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("logourl", ds.getLogourl().getValue()));
				}
				if (ds.getContactemail() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("contactemail", ds.getContactemail().getValue()));
				}
				if (ds.getNamespaceprefix() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"namespaceprefix", ds.getNamespaceprefix().getValue()));
				}
				if (ds.getLatitude() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("latitude", ds.getLatitude().getValue()));
				}
				if (ds.getLongitude() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("longitude", ds.getLongitude().getValue()));
				}
				if (ds.getDateofvalidation() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"dateofvalidation", ds.getDateofvalidation().getValue()));
				}
				if (ds.getDescription() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("description", ds.getDescription().getValue()));
				}
				if (ds.getOdnumberofitems() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"odnumberofitems", ds.getOdnumberofitems().getValue()));
				}
				if (ds.getOdnumberofitemsdate() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"odnumberofitemsdate", ds.getOdnumberofitemsdate().getValue()));
				}
				if (ds.getOdpolicies() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("odpolicies", ds.getOdpolicies().getValue()));
				}
				if (ds.getOdlanguages() != null) {
					metadata
						.addAll(
							ds
								.getOdlanguages()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("odlanguages", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (ds.getOdcontenttypes() != null) {
					metadata
						.addAll(
							ds
								.getOdcontenttypes()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("odcontenttypes", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (ds.getAccessinfopackage() != null) {
					metadata
						.addAll(
							ds
								.getAccessinfopackage()
								.stream()
								.map(c -> XmlSerializationUtils.asXmlElement("accessinfopackage", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (ds.getReleaseenddate() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"releasestartdate", ds.getReleaseenddate().getValue()));
				}
				if (ds.getReleaseenddate() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"releaseenddate", ds.getReleaseenddate().getValue()));
				}
				if (ds.getMissionstatementurl() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"missionstatementurl", ds.getMissionstatementurl().getValue()));
				}
				if (ds.getDataprovider() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"dataprovider", ds.getDataprovider().getValue().toString()));
				}
				if (ds.getServiceprovider() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"serviceprovider", ds.getServiceprovider().getValue().toString()));
				}
				if (ds.getDatabaseaccesstype() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"databaseaccesstype", ds.getDatabaseaccesstype().getValue()));
				}
				if (ds.getDatauploadtype() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"datauploadtype", ds.getDatauploadtype().getValue()));
				}
				if (ds.getDatabaseaccessrestriction() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"databaseaccessrestriction", ds.getDatabaseaccessrestriction().getValue()));
				}
				if (ds.getDatauploadrestriction() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"datauploadrestriction", ds.getDatauploadrestriction().getValue()));
				}
				if (ds.getVersioning() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"versioning", ds.getVersioning().getValue().toString()));
				}
				if (ds.getCitationguidelineurl() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"citationguidelineurl", ds.getCitationguidelineurl().getValue()));
				}
				if (ds.getQualitymanagementkind() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"qualitymanagementkind", ds.getQualitymanagementkind().getValue()));
				}
				if (ds.getPidsystems() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("pidsystems", ds.getPidsystems().getValue()));
				}
				if (ds.getCertificates() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("certificates", ds.getCertificates().getValue()));
				}
				if (ds.getPolicies() != null) {
					metadata
						.addAll(
							ds
								.getPolicies()
								.stream()
								.filter(XmlRecordFactory::kvNotBlank)
								.map(kv -> XmlSerializationUtils.mapKeyValue("policies", kv))
								.collect(Collectors.toList()));
				}
				if (ds.getJournal() != null) {
					metadata.add(XmlSerializationUtils.mapJournal(ds.getJournal()));
				}
				if (ds.getSubjects() != null) {
					metadata
						.addAll(
							ds
								.getSubjects()
								.stream()
								.filter(Objects::nonNull)
								.map(sp -> XmlSerializationUtils.mapStructuredProperty("subjects", sp))
								.collect(Collectors.toList()));
				}

				break;
			case organization:
				final Organization o = (Organization) entity;

				if (o.getLegalshortname() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"legalshortname", o.getLegalshortname().getValue()));
				}
				if (o.getLegalname() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("legalname", o.getLegalname().getValue()));
				}
				if (o.getAlternativeNames() != null) {
					metadata
						.addAll(
							o
								.getAlternativeNames()
								.stream()
								.filter(Objects::nonNull)
								.map(c -> XmlSerializationUtils.asXmlElement("alternativeNames", c.getValue()))
								.collect(Collectors.toList()));
				}
				if (o.getWebsiteurl() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("websiteurl", o.getWebsiteurl().getValue()));
				}
				if (o.getLogourl() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("logourl", o.getLogourl().getValue()));
				}

				if (o.getEclegalbody() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("eclegalbody", o.getEclegalbody().getValue()));
				}
				if (o.getEclegalperson() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("eclegalperson", o.getEclegalperson().getValue()));
				}
				if (o.getEcnonprofit() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("ecnonprofit", o.getEcnonprofit().getValue()));
				}
				if (o.getEcresearchorganization() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"ecresearchorganization", o.getEcresearchorganization().getValue()));
				}
				if (o.getEchighereducation() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"echighereducation", o.getEchighereducation().getValue()));
				}
				if (o.getEcinternationalorganizationeurinterests() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"ecinternationalorganizationeurinterests",
									o.getEcinternationalorganizationeurinterests().getValue()));
				}
				if (o.getEcinternationalorganization() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"ecinternationalorganization", o.getEcinternationalorganization().getValue()));
				}
				if (o.getEcenterprise() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("ecenterprise", o.getEcenterprise().getValue()));
				}
				if (o.getEcsmevalidated() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"ecsmevalidated", o.getEcsmevalidated().getValue()));
				}
				if (o.getEcnutscode() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("ecnutscode", o.getEcnutscode().getValue()));
				}
				if (o.getCountry() != null) {
					metadata.add(XmlSerializationUtils.mapQualifier("country", o.getCountry()));
				}

				break;
			case project:
				final Project p = (Project) entity;

				if (p.getWebsiteurl() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("websiteurl", p.getWebsiteurl().getValue()));
				}
				if (p.getCode() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("code", p.getCode().getValue()));
				}
				if (p.getAcronym() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("acronym", p.getAcronym().getValue()));
				}
				if (p.getTitle() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("title", p.getTitle().getValue()));
				}
				if (p.getStartdate() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("startdate", p.getStartdate().getValue()));
				}
				if (p.getEnddate() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("enddate", p.getEnddate().getValue()));
				}
				if (p.getCallidentifier() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"callidentifier", p.getCallidentifier().getValue()));
				}
				if (p.getKeywords() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("keywords", p.getKeywords().getValue()));
				}
				if (p.getDuration() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("duration", p.getDuration().getValue()));
				}
				if (p.getEcarticle29_3() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("ecarticle29_3", p.getEcarticle29_3().getValue()));
				}
				if (p.getSubjects() != null) {
					metadata
						.addAll(
							p
								.getSubjects()
								.stream()
								.filter(Objects::nonNull)
								.map(sp -> XmlSerializationUtils.mapStructuredProperty("subject", sp))
								.collect(Collectors.toList()));
				}
				if (p.getContracttype() != null) {
					metadata.add(XmlSerializationUtils.mapQualifier("contracttype", p.getContracttype()));
				}
				if (p.getOamandatepublications() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement("oamandatepublications", p.getOamandatepublications().getValue()));
				}
				if (p.getEcsc39() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("ecsc39", p.getEcsc39().getValue()));
				}
				if (p.getContactfullname() != null) {
					metadata
						.add(
							XmlSerializationUtils
								.asXmlElement(
									"contactfullname", p.getContactfullname().getValue()));
				}
				if (p.getContactfax() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("contactfax", p.getContactfax().getValue()));
				}
				if (p.getContactphone() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("contactphone", p.getContactphone().getValue()));
				}
				if (p.getContactemail() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("contactemail", p.getContactemail().getValue()));
				}
				if (p.getSummary() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("summary", p.getSummary().getValue()));
				}
				if (p.getCurrency() != null) {
					metadata.add(XmlSerializationUtils.asXmlElement("currency", p.getCurrency().getValue()));
				}
				if (p.getTotalcost() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("totalcost", p.getTotalcost().toString()));
				}
				if (p.getFundedamount() != null) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("fundedamount", p.getFundedamount().toString()));
				}
				if (p.getFundingtree() != null) {
					metadata
						.addAll(
							p
								.getFundingtree()
								.stream()
								.filter(Objects::nonNull)
								.map(ft -> ft.getValue())
								.collect(Collectors.toList()));
				}

				break;
			default:
				throw new IllegalArgumentException("invalid entity type: " + type);
		}

		return metadata;
	}

	private String getAuthorPidType(String s) {
		return XmlSerializationUtils
			.escapeXml(s)
			.replaceAll("\\W", "")
			.replaceAll("\\d", "");
	}

	private static boolean kvNotBlank(KeyValue kv) {
		return kv != null && StringUtils.isNotBlank(kv.getKey()) && StringUtils.isNotBlank(kv.getValue());
	}

	private void mapDatasourceType(List<String> metadata, final Qualifier dsType) {
		metadata.add(XmlSerializationUtils.mapQualifier("datasourcetype", dsType));

		if (specialDatasourceTypes.contains(dsType.getClassid())) {
			dsType.setClassid("other");
			dsType.setClassname("other");
		}
		metadata.add(XmlSerializationUtils.mapQualifier("datasourcetypeui", dsType));
	}

	private List<String> mapFields(RelatedEntityWrapper link, Set<String> contexts) {
		final Relation rel = link.getRelation();
		final RelatedEntity re = link.getTarget();
		final String targetType = link.getTarget().getType();

		final List<String> metadata = Lists.newArrayList();
		switch (EntityType.valueOf(targetType)) {
			case publication:
			case dataset:
			case otherresearchproduct:
			case software:
				if (re.getTitle() != null && isNotBlank(re.getTitle().getValue())) {
					metadata.add(XmlSerializationUtils.mapStructuredProperty("title", re.getTitle()));
				}
				if (isNotBlank(re.getDateofacceptance())) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("dateofacceptance", re.getDateofacceptance()));
				}
				if (isNotBlank(re.getPublisher())) {
					metadata.add(XmlSerializationUtils.asXmlElement("publisher", re.getPublisher()));
				}
				if (isNotBlank(re.getCodeRepositoryUrl())) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("coderepositoryurl", re.getCodeRepositoryUrl()));
				}
				if (re.getResulttype() != null && re.getResulttype().isBlank()) {
					metadata.add(XmlSerializationUtils.mapQualifier("resulttype", re.getResulttype()));
				}
				if (re.getCollectedfrom() != null) {
					metadata
						.addAll(
							re
								.getCollectedfrom()
								.stream()
								.filter(XmlRecordFactory::kvNotBlank)
								.map(kv -> XmlSerializationUtils.mapKeyValue("collectedfrom", kv))
								.collect(Collectors.toList()));
				}
				if (re.getPid() != null) {
					metadata
						.addAll(
							re
								.getPid()
								.stream()
								.map(p -> XmlSerializationUtils.mapStructuredProperty("pid", p))
								.collect(Collectors.toList()));
				}
				break;
			case datasource:
				if (isNotBlank(re.getOfficialname())) {
					metadata.add(XmlSerializationUtils.asXmlElement("officialname", re.getOfficialname()));
				}
				if (re.getDatasourcetype() != null && !re.getDatasourcetype().isBlank()) {
					mapDatasourceType(metadata, re.getDatasourcetype());
				}
				if (re.getOpenairecompatibility() != null && !re.getOpenairecompatibility().isBlank()) {
					metadata
						.add(
							XmlSerializationUtils
								.mapQualifier(
									"openairecompatibility", re.getOpenairecompatibility()));
				}
				break;
			case organization:
				if (isNotBlank(re.getLegalname())) {
					metadata.add(XmlSerializationUtils.asXmlElement("legalname", re.getLegalname()));
				}
				if (isNotBlank(re.getLegalshortname())) {
					metadata
						.add(
							XmlSerializationUtils.asXmlElement("legalshortname", re.getLegalshortname()));
				}
				if (re.getCountry() != null && !re.getCountry().isBlank()) {
					metadata.add(XmlSerializationUtils.mapQualifier("country", re.getCountry()));
				}
				break;
			case project:
				if (isNotBlank(re.getProjectTitle())) {
					metadata.add(XmlSerializationUtils.asXmlElement("title", re.getProjectTitle()));
				}
				if (isNotBlank(re.getCode())) {
					metadata.add(XmlSerializationUtils.asXmlElement("code", re.getCode()));
				}
				if (isNotBlank(re.getAcronym())) {
					metadata.add(XmlSerializationUtils.asXmlElement("acronym", re.getAcronym()));
				}
				if (re.getContracttype() != null && !re.getContracttype().isBlank()) {
					metadata.add(XmlSerializationUtils.mapQualifier("contracttype", re.getContracttype()));
				}
				if (re.getFundingtree() != null && contexts != null) {
					metadata
						.addAll(
							re
								.getFundingtree()
								.stream()
								.peek(ft -> fillContextMap(ft, contexts))
								.map(ft -> getRelFundingTree(ft))
								.collect(Collectors.toList()));
				}
				break;
			default:
				throw new IllegalArgumentException("invalid target type: " + targetType);
		}

		final String accumulatorName = getRelDescriptor(rel.getRelType(), rel.getSubRelType(), rel.getRelClass());
		if (accumulators.containsKey(accumulatorName)) {
			accumulators.get(accumulatorName).add(1);
		}

		return metadata;
	}

	private String mapRelation(Set<String> contexts, TemplateFactory templateFactory, EntityType type,
		RelatedEntityWrapper link) {
		final Relation rel = link.getRelation();
		final String targetType = link.getTarget().getType();
		final String scheme = ModelSupport.getScheme(type.toString(), targetType);

		if (StringUtils.isBlank(scheme)) {
			throw new IllegalArgumentException(
				String.format("missing scheme for: <%s - %s>", type.toString(), targetType));
		}
		final HashSet<String> fields = Sets.newHashSet(mapFields(link, contexts));
		return templateFactory
			.getRel(
				targetType, rel.getTarget(), fields, rel.getRelClass(), scheme, rel.getDataInfo());
	}

	private List<String> listChildren(
		final OafEntity entity, JoinedEntity je, TemplateFactory templateFactory) {

		final EntityType entityType = EntityType.fromClass(je.getEntity().getClass());

		final List<RelatedEntityWrapper> links = je.getLinks();
		List<String> children = links
			.stream()
			.filter(link -> isDuplicate(link))
			.map(link -> {
				final String targetType = link.getTarget().getType();
				final String name = ModelSupport.getMainType(EntityType.valueOf(targetType));
				final HashSet<String> fields = Sets.newHashSet(mapFields(link, null));
				return templateFactory
					.getChild(name, link.getTarget().getId(), Lists.newArrayList(fields));
			})
			.collect(Collectors.toCollection(ArrayList::new));

		if (MainEntityType.result.toString().equals(ModelSupport.getMainType(entityType))) {
			final List<Instance> instances = ((Result) entity).getInstance();
			if (instances != null) {
				for (final Instance instance : ((Result) entity).getInstance()) {

					final List<String> fields = Lists.newArrayList();

					if (instance.getAccessright() != null && !instance.getAccessright().isBlank()) {
						fields
							.add(
								XmlSerializationUtils.mapQualifier("accessright", instance.getAccessright()));
					}
					if (instance.getCollectedfrom() != null && kvNotBlank(instance.getCollectedfrom())) {
						fields
							.add(
								XmlSerializationUtils.mapKeyValue("collectedfrom", instance.getCollectedfrom()));
					}
					if (instance.getHostedby() != null && kvNotBlank(instance.getHostedby())) {
						fields.add(XmlSerializationUtils.mapKeyValue("hostedby", instance.getHostedby()));
					}
					if (instance.getDateofacceptance() != null
						&& isNotBlank(instance.getDateofacceptance().getValue())) {
						fields
							.add(
								XmlSerializationUtils
									.asXmlElement(
										"dateofacceptance", instance.getDateofacceptance().getValue()));
					}
					if (instance.getInstancetype() != null && !instance.getInstancetype().isBlank()) {
						fields
							.add(
								XmlSerializationUtils.mapQualifier("instancetype", instance.getInstancetype()));
					}
					if (isNotBlank(instance.getDistributionlocation())) {
						fields
							.add(
								XmlSerializationUtils
									.asXmlElement(
										"distributionlocation", instance.getDistributionlocation()));
					}
					if (instance.getRefereed() != null && !instance.getRefereed().isBlank()) {
						fields
							.add(
								XmlSerializationUtils.mapQualifier("refereed", instance.getRefereed()));
					}
					if (instance.getProcessingchargeamount() != null
						&& isNotBlank(instance.getProcessingchargeamount().getValue())) {
						fields
							.add(
								XmlSerializationUtils
									.asXmlElement(
										"processingchargeamount", instance.getProcessingchargeamount().getValue()));
					}
					if (instance.getProcessingchargecurrency() != null
						&& isNotBlank(instance.getProcessingchargecurrency().getValue())) {
						fields
							.add(
								XmlSerializationUtils
									.asXmlElement(
										"processingchargecurrency", instance.getProcessingchargecurrency().getValue()));
					}

					children
						.add(
							templateFactory
								.getInstance(
									instance.getHostedby().getKey(), fields, instance.getUrl()));
				}
			}
			final List<ExternalReference> ext = ((Result) entity).getExternalReference();
			if (ext != null) {
				for (final ExternalReference er : ((Result) entity).getExternalReference()) {

					final List<String> fields = Lists.newArrayList();

					if (isNotBlank(er.getSitename())) {
						fields.add(XmlSerializationUtils.asXmlElement("sitename", er.getSitename()));
					}
					if (isNotBlank(er.getLabel())) {
						fields.add(XmlSerializationUtils.asXmlElement("label", er.getLabel()));
					}
					if (isNotBlank(er.getUrl())) {
						fields.add(XmlSerializationUtils.asXmlElement("url", er.getUrl()));
					}
					if (isNotBlank(er.getDescription())) {
						fields.add(XmlSerializationUtils.asXmlElement("description", er.getDescription()));
					}
					if (isNotBlank(er.getUrl())) {
						fields.add(XmlSerializationUtils.mapQualifier("qualifier", er.getQualifier()));
					}
					if (isNotBlank(er.getRefidentifier())) {
						fields.add(XmlSerializationUtils.asXmlElement("refidentifier", er.getRefidentifier()));
					}
					if (isNotBlank(er.getQuery())) {
						fields.add(XmlSerializationUtils.asXmlElement("query", er.getQuery()));
					}

					children.add(templateFactory.getChild("externalreference", null, fields));
				}
			}
		}

		return children;
	}

	private boolean isDuplicate(RelatedEntityWrapper link) {
		return REL_SUBTYPE_DEDUP.equalsIgnoreCase(link.getRelation().getSubRelType());
	}

	private List<String> listExtraInfo(OafEntity entity) {
		final List<ExtraInfo> extraInfo = entity.getExtraInfo();
		return extraInfo != null
			? extraInfo
				.stream()
				.map(e -> XmlSerializationUtils.mapExtraInfo(e))
				.collect(Collectors.toList())
			: Lists.newArrayList();
	}

	private List<String> buildContexts(final String type, final Set<String> contexts) {
		final List<String> res = Lists.newArrayList();

		if ((contextMapper != null)
			&& !contextMapper.isEmpty()
			&& MainEntityType.result.toString().equals(type)) {

			XMLTag document = XMLDoc.newDocument(true).addRoot("contextRoot");

			for (final String context : contexts) {

				String id = "";
				for (final String token : Splitter.on("::").split(context)) {
					id += token;

					final ContextDef def = contextMapper.get(id);

					if (def == null) {
						continue;
						// throw new IllegalStateException(String.format("cannot find context for id
						// '%s'",
						// id));
					}

					if (def.getName().equals("context")) {
						final String xpath = "//context/@id='" + def.getId() + "'";
						if (!document.gotoRoot().rawXpathBoolean(xpath, new Object())) {
							document = addContextDef(document.gotoRoot(), def);
						}
					}

					if (def.getName().equals("category")) {
						final String rootId = substringBefore(def.getId(), "::");
						document = addContextDef(
							document.gotoRoot().gotoTag("//context[./@id='" + rootId + "']", new Object()),
							def);
					}

					if (def.getName().equals("concept")) {
						document = addContextDef(document, def).gotoParent();
					}
					id += "::";
				}
			}
			final Transformer transformer = getTransformer();
			for (final org.w3c.dom.Element x : document.gotoRoot().getChildElement()) {
				try {
					res.add(asStringElement(x, transformer));
				} catch (final TransformerException e) {
					throw new RuntimeException(e);
				}
			}
		}

		return res;
	}

	private Transformer getTransformer() {
		try {
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			return transformer;
		} catch (TransformerConfigurationException e) {
			throw new IllegalStateException("unable to create javax.xml.transform.Transformer", e);
		}
	}

	private XMLTag addContextDef(final XMLTag tag, final ContextDef def) {
		tag.addTag(def.getName()).addAttribute("id", def.getId()).addAttribute("label", def.getLabel());
		if ((def.getType() != null) && !def.getType().isEmpty()) {
			tag.addAttribute("type", def.getType());
		}
		return tag;
	}

	private String asStringElement(final org.w3c.dom.Element element, final Transformer transformer)
		throws TransformerException {
		final StringWriter buffer = new StringWriter();
		transformer.transform(new DOMSource(element), new StreamResult(buffer));
		return buffer.toString();
	}

	private void fillContextMap(final String xmlTree, final Set<String> contexts) {

		Document fundingPath;
		try {
			fundingPath = new SAXReader().read(new StringReader(xmlTree));
		} catch (final DocumentException e) {
			throw new RuntimeException(e);
		}
		try {
			final Node funder = fundingPath.selectSingleNode("//funder");

			if (funder != null) {

				final String funderShortName = funder.valueOf("./shortname");
				contexts.add(funderShortName);

				contextMapper
					.put(
						funderShortName,
						new ContextDef(funderShortName, funder.valueOf("./name"), "context", "funding"));
				final Node level0 = fundingPath.selectSingleNode("//funding_level_0");
				if (level0 != null) {
					final String level0Id = Joiner.on("::").join(funderShortName, level0.valueOf("./name"));
					contextMapper
						.put(
							level0Id, new ContextDef(level0Id, level0.valueOf("./description"), "category", ""));
					final Node level1 = fundingPath.selectSingleNode("//funding_level_1");
					if (level1 == null) {
						contexts.add(level0Id);
					} else {
						final String level1Id = Joiner.on("::").join(level0Id, level1.valueOf("./name"));
						contextMapper
							.put(
								level1Id, new ContextDef(level1Id, level1.valueOf("./description"), "concept", ""));
						final Node level2 = fundingPath.selectSingleNode("//funding_level_2");
						if (level2 == null) {
							contexts.add(level1Id);
						} else {
							final String level2Id = Joiner.on("::").join(level1Id, level2.valueOf("./name"));
							contextMapper
								.put(
									level2Id,
									new ContextDef(level2Id, level2.valueOf("./description"), "concept", ""));
							contexts.add(level2Id);
						}
					}
				}
			}
		} catch (final NullPointerException e) {
			throw new IllegalArgumentException("malformed funding path: " + xmlTree, e);
		}
	}

	@SuppressWarnings("unchecked")
	protected static String getRelFundingTree(final String xmlTree) {
		String funding = "<funding>";
		try {
			final Document ftree = new SAXReader().read(new StringReader(xmlTree));
			funding = "<funding>";

			funding += getFunderElement(ftree);

			for (final Object o : Lists
				.reverse(
					ftree.selectNodes("//fundingtree//*[starts-with(local-name(),'funding_level_')]"))) {
				final Element e = (Element) o;
				final String _id = e.valueOf("./id");
				funding += "<"
					+ e.getName()
					+ " name=\""
					+ XmlSerializationUtils.escapeXml(e.valueOf("./name"))
					+ "\">"
					+ XmlSerializationUtils.escapeXml(_id)
					+ "</"
					+ e.getName()
					+ ">";
			}
		} catch (final DocumentException e) {
			throw new IllegalArgumentException(
				"unable to parse funding tree: " + xmlTree + "\n" + e.getMessage());
		} finally {
			funding += "</funding>";
		}
		return funding;
	}

	private static String getFunderElement(final Document ftree) {
		final String funderId = ftree.valueOf("//fundingtree/funder/id");
		final String funderShortName = ftree.valueOf("//fundingtree/funder/shortname");
		final String funderName = ftree.valueOf("//fundingtree/funder/name");
		final String funderJurisdiction = ftree.valueOf("//fundingtree/funder/jurisdiction");

		return "<funder id=\""
			+ XmlSerializationUtils.escapeXml(funderId)
			+ "\" shortname=\""
			+ XmlSerializationUtils.escapeXml(funderShortName)
			+ "\" name=\""
			+ XmlSerializationUtils.escapeXml(funderName)
			+ "\" jurisdiction=\""
			+ XmlSerializationUtils.escapeXml(funderJurisdiction)
			+ "\" />";
	}
}
