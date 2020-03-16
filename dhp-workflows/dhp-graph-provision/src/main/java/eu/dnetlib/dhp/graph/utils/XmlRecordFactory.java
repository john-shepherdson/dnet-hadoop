package eu.dnetlib.dhp.graph.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;
import eu.dnetlib.dhp.graph.model.JoinedEntity;
import eu.dnetlib.dhp.graph.model.RelatedEntity;
import eu.dnetlib.dhp.graph.model.Tuple2;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.graph.utils.GraphMappingUtils.*;
import static eu.dnetlib.dhp.graph.utils.XmlSerializationUtils.*;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringBefore;

public class XmlRecordFactory implements Serializable {

    private  Map<String, LongAccumulator> accumulators;

    private Set<String> specialDatasourceTypes;

    private ContextMapper contextMapper;

    private String schemaLocation;

    private boolean indent = false;

    public XmlRecordFactory(
            final ContextMapper contextMapper, final boolean indent,
            final String schemaLocation, final String otherDatasourceTypesUForUI) {

        this(Maps.newHashMap(), contextMapper, indent, schemaLocation, otherDatasourceTypesUForUI);
    }

    public XmlRecordFactory(
            final  Map<String, LongAccumulator> accumulators,
            final ContextMapper contextMapper, final boolean indent,
            final String schemaLocation, final String otherDatasourceTypesUForUI) {

        this.accumulators = accumulators;
        this.contextMapper = contextMapper;
        this.schemaLocation = schemaLocation;
        this.specialDatasourceTypes = Sets.newHashSet(Splitter.on(",").trimResults().split(otherDatasourceTypesUForUI));

        this.indent = indent;
    }

    public String build(final JoinedEntity je) {

        final Set<String> contexts = Sets.newHashSet();

        final OafEntity entity = je.getEntity();
        TemplateFactory templateFactory = new TemplateFactory();
        try {
            final List<String> metadata = metadata(je.getType(), entity, contexts);

            // rels has to be processed before the contexts because they enrich the contextMap with the funding info.
            final List<String> relations = listRelations(je, templateFactory, contexts);

            metadata.addAll(buildContexts(getMainType(je.getType()), contexts));
            metadata.add(parseDataInfo(entity.getDataInfo()));

            final String body = templateFactory.buildBody(
                    getMainType(je.getType()),
                    metadata,
                    relations,
                    listChildren(je, templateFactory), listExtraInfo(je));

            return printXML(templateFactory.buildRecord(entity, schemaLocation, body), indent);
        } catch (final Throwable e) {
            throw new RuntimeException(String.format("error building record '%s'", entity.getId()), e);
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

    private List<String> metadata(final String type, final OafEntity entity, final Set<String> contexts) {

        final List<String> metadata = Lists.newArrayList();


        if (entity.getCollectedfrom() != null) {
            metadata.addAll(entity.getCollectedfrom()
                .stream()
                .map(kv -> mapKeyValue("collectedfrom", kv))
                .collect(Collectors.toList()));
        }
        if (entity.getOriginalId() != null) {
            metadata.addAll(entity.getOriginalId()
                .stream()
                .map(s -> asXmlElement("originalId", s))
                .collect(Collectors.toList()));
        }
        if (entity.getPid() != null) {
            metadata.addAll(entity.getPid()
                .stream()
                .map(p -> mapStructuredProperty("pid", p))
                .collect(Collectors.toList()));
        }

        if (GraphMappingUtils.isResult(type)) {
            final Result r = (Result) entity;

            if (r.getContext() != null) {
                contexts.addAll(r.getContext()
                        .stream()
                        .map(c -> c.getId())
                        .collect(Collectors.toList()));
                /* FIXME: Workaround for CLARIN mining issue: #3670#note-29 */
                if (contexts.contains("dh-ch::subcommunity::2")) {
                    contexts.add("clarin");
                }
            }

            if (r.getTitle() != null) {
                metadata.addAll(r.getTitle()
                    .stream()
                    .map(t -> mapStructuredProperty("title", t))
                    .collect(Collectors.toList()));
            }
            if (r.getBestaccessright() != null) {
                metadata.add(mapQualifier("bestaccessright", r.getBestaccessright()));
            }
            if (r.getAuthor() != null) {
                metadata.addAll(r.getAuthor()
                    .stream()
                    .map(a -> {
                        final StringBuilder sb = new StringBuilder("<creator rank=\"" + a.getRank() + "\"");
                        if (isNotBlank(a.getName())) {
                            sb.append(" name=\"" + escapeXml(a.getName()) + "\"");
                        }
                        if (isNotBlank(a.getSurname())) {
                            sb.append(" surname=\"" + escapeXml(a.getSurname()) + "\"");
                        }
                        if (a.getPid() != null) {
                            a.getPid().stream()
                                    .filter(sp -> isNotBlank(sp.getQualifier().getClassid()) && isNotBlank(sp.getValue()))
                                    .forEach(sp -> {
                                        String pidType = escapeXml(sp.getQualifier().getClassid()).replaceAll("\\W", "");
                                        String pidValue = escapeXml(sp.getValue());

                                        // ugly hack: some records provide swapped pidtype and pidvalue
                                        if (authorPidTypes.contains(pidValue.toLowerCase().trim())) {
                                            sb.append(String.format(" %s=\"%s\"", pidValue, pidType));
                                        } else {
                                            pidType = pidType.replaceAll("\\W", "").replaceAll("\\d", "");
                                            if (isNotBlank(pidType)) {
                                                sb.append(String.format(" %s=\"%s\"",
                                                        pidType,
                                                        pidValue.toLowerCase().replaceAll("orcid", "")));
                                            }
                                        }
                                    });
                        }
                        sb.append(">" + escapeXml(a.getFullname()) + "</creator>");
                        return sb.toString();
                    }).collect(Collectors.toList()));
            }
            if (r.getContributor() != null) {
                metadata.addAll(r.getContributor()
                    .stream()
                    .map(c -> asXmlElement("contributor", c.getValue()))
                    .collect(Collectors.toList()));
            }
            if (r.getCountry() != null) {
                metadata.addAll(r.getCountry()
                    .stream()
                    .map(c -> mapQualifier("country", c))
                    .collect(Collectors.toList()));
            }
            if (r.getCoverage() != null) {
                metadata.addAll(r.getCoverage()
                    .stream()
                    .map(c -> asXmlElement("coverage", c.getValue()))
                    .collect(Collectors.toList()));
            }
            if (r.getDateofacceptance() != null) {
                metadata.add(asXmlElement("dateofacceptance", r.getDateofacceptance().getValue()));
            }
            if (r.getDescription() != null) {
                metadata.addAll(r.getDescription()
                        .stream()
                        .map(c -> asXmlElement("description", c.getValue()))
                        .collect(Collectors.toList()));
            }
            if (r.getEmbargoenddate() != null) {
                metadata.add(asXmlElement("embargoenddate", r.getEmbargoenddate().getValue()));
            }
            if (r.getSubject() != null) {
                metadata.addAll(r.getSubject()
                    .stream()
                    .map(s -> mapStructuredProperty("subject", s))
                    .collect(Collectors.toList()));
            }
            if (r.getLanguage() != null) {
                metadata.add(mapQualifier("language", r.getLanguage()));
            }
            if (r.getRelevantdate() != null) {
                metadata.addAll(r.getRelevantdate()
                    .stream()
                    .map(s -> mapStructuredProperty("relevantdate", s))
                    .collect(Collectors.toList()));
            }
            if (r.getPublisher() != null) {
                metadata.add(asXmlElement("publisher", r.getPublisher().getValue()));
            }
            if (r.getSource() != null) {
                metadata.addAll(r.getSource()
                    .stream()
                    .map(c -> asXmlElement("source", c.getValue()))
                    .collect(Collectors.toList()));
            }
            if (r.getFormat() != null) {
                metadata.addAll(r.getFormat()
                    .stream()
                    .map(c -> asXmlElement("format", c.getValue()))
                    .collect(Collectors.toList()));
            }
            if (r.getResulttype() != null) {
                metadata.add(mapQualifier("resulttype", r.getResulttype()));
            }
            if (r.getResourcetype() != null) {
                metadata.add(mapQualifier("resourcetype", r.getResourcetype()));
            }

            metadata.add(mapQualifier("bestaccessright", getBestAccessright(r)));
        }

        switch (EntityType.valueOf(type)) {
            case publication:
                final Publication pub = (Publication) entity;

                if (pub.getJournal() != null) {
                    final Journal j = pub.getJournal();
                    metadata.add(mapJournal(j));
                }

                break;
            case dataset:
                final Dataset d = (Dataset) entity;
                if (d.getDevice() != null) {
                    metadata.add(asXmlElement("device", d.getDevice().getValue()));
                }
                if (d.getLastmetadataupdate() != null) {
                    metadata.add(asXmlElement("lastmetadataupdate", d.getLastmetadataupdate().getValue()));
                }
                if (d.getMetadataversionnumber() != null) {
                    metadata.add(asXmlElement("metadataversionnumber", d.getMetadataversionnumber().getValue()));
                }
                if (d.getSize() != null) {
                    metadata.add(asXmlElement("size", d.getSize().getValue()));
                }
                if (d.getStoragedate() != null) {
                    metadata.add(asXmlElement("storagedate", d.getStoragedate().getValue()));
                }
                if (d.getVersion() != null) {
                    metadata.add(asXmlElement("version", d.getVersion().getValue()));
                }
                //TODO d.getGeolocation()

                break;
            case otherresearchproduct:
                final OtherResearchProduct orp = (OtherResearchProduct) entity;

                if (orp.getContactperson() != null) {
                    metadata.addAll(orp.getContactperson()
                            .stream()
                            .map(c -> asXmlElement("contactperson", c.getValue()))
                            .collect(Collectors.toList()));
                }

                if (orp.getContactgroup() != null) {
                    metadata.addAll(orp.getContactgroup()
                            .stream()
                            .map(c -> asXmlElement("contactgroup", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (orp.getTool() != null) {
                    metadata.addAll(orp.getTool()
                            .stream()
                            .map(c -> asXmlElement("tool", c.getValue()))
                            .collect(Collectors.toList()));
                }
                break;
            case software:
                final Software s = (Software) entity;

                if (s.getDocumentationUrl() != null) {
                    metadata.addAll(s.getDocumentationUrl()
                            .stream()
                            .map(c -> asXmlElement("documentationUrl", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (s.getLicense() != null) {
                    metadata.addAll(s.getLicense()
                            .stream()
                            .map(l -> mapStructuredProperty("license", l))
                            .collect(Collectors.toList()));
                }
                if (s.getCodeRepositoryUrl() != null) {
                    metadata.add(asXmlElement("codeRepositoryUrl", s.getCodeRepositoryUrl().getValue()));
                }
                if (s.getProgrammingLanguage() != null) {
                    metadata.add(mapQualifier("programmingLanguage", s.getProgrammingLanguage()));
                }
                break;
            case datasource:
                final Datasource ds = (Datasource) entity;

                if (ds.getDatasourcetype() != null) {
                    mapDatasourceType(metadata, ds.getDatasourcetype());
                }
                if (ds.getOpenairecompatibility() != null) {
                    metadata.add(mapQualifier("openairecompatibility", ds.getOpenairecompatibility()));
                }
                if (ds.getOfficialname() != null) {
                    metadata.add(asXmlElement("officialname", ds.getOfficialname().getValue()));
                }
                if (ds.getEnglishname() != null) {
                    metadata.add(asXmlElement("englishname", ds.getEnglishname().getValue()));
                }
                if (ds.getWebsiteurl() != null) {
                    metadata.add(asXmlElement("websiteurl", ds.getWebsiteurl().getValue()));
                }
                if (ds.getLogourl() != null) {
                    metadata.add(asXmlElement("logourl", ds.getLogourl().getValue()));
                }
                if (ds.getContactemail() != null) {
                    metadata.add(asXmlElement("contactemail", ds.getContactemail().getValue()));
                }
                if (ds.getNamespaceprefix() != null) {
                    metadata.add(asXmlElement("namespaceprefix", ds.getNamespaceprefix().getValue()));
                }
                if (ds.getLatitude() != null) {
                    metadata.add(asXmlElement("latitude", ds.getLatitude().getValue()));
                }
                if (ds.getLongitude() != null) {
                    metadata.add(asXmlElement("longitude", ds.getLongitude().getValue()));
                }
                if (ds.getDateofvalidation() != null) {
                    metadata.add(asXmlElement("dateofvalidation", ds.getDateofvalidation().getValue()));
                }
                if (ds.getDescription() != null) {
                    metadata.add(asXmlElement("description", ds.getDescription().getValue()));
                }
                if (ds.getOdnumberofitems() != null) {
                    metadata.add(asXmlElement("odnumberofitems", ds.getOdnumberofitems().getValue()));
                }
                if (ds.getOdnumberofitemsdate() != null) {
                    metadata.add(asXmlElement("odnumberofitemsdate", ds.getOdnumberofitemsdate().getValue()));
                }
                if (ds.getOdpolicies() != null) {
                    metadata.add(asXmlElement("odpolicies", ds.getOdpolicies().getValue()));
                }
                if (ds.getOdlanguages() != null) {
                    metadata.addAll(ds.getOdlanguages()
                            .stream()
                            .map(c -> asXmlElement("odlanguages", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (ds.getOdcontenttypes() != null) {
                    metadata.addAll(ds.getOdcontenttypes()
                            .stream()
                            .map(c -> asXmlElement("odcontenttypes", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (ds.getAccessinfopackage() != null) {
                    metadata.addAll(ds.getAccessinfopackage()
                            .stream()
                            .map(c -> asXmlElement("accessinfopackage", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (ds.getReleaseenddate() != null) {
                    metadata.add(asXmlElement("releasestartdate", ds.getReleaseenddate().getValue()));
                }
                if (ds.getReleaseenddate() != null) {
                    metadata.add(asXmlElement("releaseenddate", ds.getReleaseenddate().getValue()));
                }
                if (ds.getMissionstatementurl() != null) {
                    metadata.add(asXmlElement("missionstatementurl", ds.getMissionstatementurl().getValue()));
                }
                if (ds.getDataprovider() != null) {
                    metadata.add(asXmlElement("dataprovider", ds.getDataprovider().getValue().toString()));
                }
                if (ds.getServiceprovider() != null) {
                    metadata.add(asXmlElement("serviceprovider", ds.getServiceprovider().getValue().toString()));
                }
                if (ds.getDatabaseaccesstype() != null) {
                    metadata.add(asXmlElement("databaseaccesstype", ds.getDatabaseaccesstype().getValue()));
                }
                if (ds.getDatauploadtype() != null) {
                    metadata.add(asXmlElement("datauploadtype", ds.getDatauploadtype().getValue()));
                }
                if (ds.getDatabaseaccessrestriction() != null) {
                    metadata.add(asXmlElement("databaseaccessrestriction", ds.getDatabaseaccessrestriction().getValue()));
                }
                if (ds.getDatauploadrestriction() != null) {
                    metadata.add(asXmlElement("datauploadrestriction", ds.getDatauploadrestriction().getValue()));
                }
                if (ds.getVersioning() != null) {
                    metadata.add(asXmlElement("versioning", ds.getVersioning().getValue().toString()));
                }
                if (ds.getCitationguidelineurl() != null) {
                    metadata.add(asXmlElement("citationguidelineurl", ds.getCitationguidelineurl().getValue()));
                }
                if (ds.getQualitymanagementkind() != null) {
                    metadata.add(asXmlElement("qualitymanagementkind", ds.getQualitymanagementkind().getValue()));
                }
                if (ds.getPidsystems() != null) {
                    metadata.add(asXmlElement("pidsystems", ds.getPidsystems().getValue()));
                }
                if (ds.getCertificates() != null) {
                    metadata.add(asXmlElement("certificates", ds.getCertificates().getValue()));
                }
                if (ds.getPolicies() != null) {
                    metadata.addAll(ds.getPolicies()
                            .stream()
                            .map(kv -> mapKeyValue("policies", kv))
                            .collect(Collectors.toList()));
                }
                if (ds.getJournal() != null) {
                    metadata.add(mapJournal(ds.getJournal()));
                }
                if (ds.getSubjects() != null) {
                    metadata.addAll(ds.getSubjects()
                            .stream()
                            .map(sp -> mapStructuredProperty("subjects", sp))
                            .collect(Collectors.toList()));
                }

                break;
            case organization:
                final Organization o = (Organization) entity;

                if (o.getLegalshortname() != null) {
                    metadata.add(asXmlElement("legalshortname", o.getLegalshortname().getValue()));
                }
                if (o.getLegalname() != null) {
                    metadata.add(asXmlElement("legalname", o.getLegalname().getValue()));
                }
                if (o.getAlternativeNames() != null) {
                    metadata.addAll(o.getAlternativeNames()
                            .stream()
                            .map(c -> asXmlElement("alternativeNames", c.getValue()))
                            .collect(Collectors.toList()));
                }
                if (o.getWebsiteurl() != null) {
                    metadata.add(asXmlElement("websiteurl", o.getWebsiteurl().getValue()));
                }
                if (o.getLogourl() != null) {
                    metadata.add(asXmlElement("websiteurl", o.getLogourl().getValue()));
                }

                if (o.getEclegalbody() != null) {
                    metadata.add(asXmlElement("eclegalbody", o.getEclegalbody().getValue()));
                }
                if (o.getEclegalperson() != null) {
                    metadata.add(asXmlElement("eclegalperson", o.getEclegalperson().getValue()));
                }
                if (o.getEcnonprofit() != null) {
                    metadata.add(asXmlElement("ecnonprofit", o.getEcnonprofit().getValue()));
                }
                if (o.getEcresearchorganization() != null) {
                    metadata.add(asXmlElement("ecresearchorganization", o.getEcresearchorganization().getValue()));
                }
                if (o.getEchighereducation() != null) {
                    metadata.add(asXmlElement("echighereducation", o.getEchighereducation().getValue()));
                }
                if (o.getEcinternationalorganization() != null) {
                    metadata.add(asXmlElement("ecinternationalorganizationeurinterests", o.getEcinternationalorganization().getValue()));
                }
                if (o.getEcinternationalorganization() != null) {
                    metadata.add(asXmlElement("ecinternationalorganization", o.getEcinternationalorganization().getValue()));
                }
                if (o.getEcenterprise() != null) {
                    metadata.add(asXmlElement("ecenterprise", o.getEcenterprise().getValue()));
                }
                if (o.getEcsmevalidated() != null) {
                    metadata.add(asXmlElement("ecsmevalidated", o.getEcsmevalidated().getValue()));
                }
                if (o.getEcnutscode() != null) {
                    metadata.add(asXmlElement("ecnutscode", o.getEcnutscode().getValue()));
                }
                if (o.getCountry() != null) {
                    metadata.add(mapQualifier("country", o.getCountry()));
                }

                break;
            case project:

                final Project p = (Project) entity;

                if (p.getWebsiteurl() != null) {
                    metadata.add(asXmlElement("websiteurl", p.getWebsiteurl().getValue()));
                }
                if (p.getCode() != null) {
                    metadata.add(asXmlElement("code", p.getCode().getValue()));
                }
                if (p.getAcronym() != null) {
                    metadata.add(asXmlElement("acronym", p.getAcronym().getValue()));
                }
                if (p.getTitle() != null) {
                    metadata.add(asXmlElement("title", p.getTitle().getValue()));
                }
                if (p.getStartdate() != null) {
                    metadata.add(asXmlElement("startdate", p.getStartdate().getValue()));
                }
                if (p.getEnddate() != null) {
                    metadata.add(asXmlElement("enddate", p.getEnddate().getValue()));
                }
                if (p.getCallidentifier() != null) {
                    metadata.add(asXmlElement("callidentifier", p.getCallidentifier().getValue()));
                }
                if (p.getKeywords() != null) {
                    metadata.add(asXmlElement("keywords", p.getKeywords().getValue()));
                }
                if (p.getDuration() != null) {
                    metadata.add(asXmlElement("duration", p.getDuration().getValue()));
                }
                if (p.getEcarticle29_3() != null) {
                    metadata.add(asXmlElement("ecarticle29_3", p.getEcarticle29_3().getValue()));
                }
                if (p.getSubjects() != null) {
                    metadata.addAll(p.getSubjects()
                            .stream()
                            .map(sp -> mapStructuredProperty("subject", sp))
                            .collect(Collectors.toList()));
                }
                if (p.getContracttype() != null) {
                    metadata.add(mapQualifier("contracttype", p.getContracttype()));
                }
                if (p.getEcsc39() != null) {
                    metadata.add(asXmlElement("ecsc39", p.getEcsc39().getValue()));
                }
                if (p.getContactfullname() != null) {
                    metadata.add(asXmlElement("contactfullname", p.getContactfullname().getValue()));
                }
                if (p.getContactfax() != null) {
                    metadata.add(asXmlElement("contactfax", p.getContactfax().getValue()));
                }
                if (p.getContactphone() != null) {
                    metadata.add(asXmlElement("contactphone", p.getContactphone().getValue()));
                }
                if (p.getContactemail() != null) {
                    metadata.add(asXmlElement("contactemail", p.getContactemail().getValue()));
                }
                if (p.getSummary() != null) {
                    metadata.add(asXmlElement("summary", p.getSummary().getValue()));
                }
                if (p.getCurrency() != null) {
                    metadata.add(asXmlElement("currency", p.getCurrency().getValue()));
                }
                if (p.getTotalcost() != null) {
                    metadata.add(asXmlElement("totalcost", p.getTotalcost().toString()));
                }
                if (p.getFundedamount() != null) {
                    metadata.add(asXmlElement("fundedamount", p.getFundedamount().toString()));
                }
                if (p.getFundingtree() != null) {
                    metadata.addAll(p.getFundingtree()
                            .stream()
                            .map(ft -> ft.getValue())
                            .collect(Collectors.toList()));
                }

                break;
            default:
                throw new IllegalArgumentException("invalid entity type: " + type);
        }

        return metadata;
    }

    private void mapDatasourceType(List<String> metadata, final Qualifier dsType) {
        metadata.add(mapQualifier("datasourcetype", dsType));

        if (specialDatasourceTypes.contains(dsType.getClassid())) {
            dsType.setClassid("other");
            dsType.setClassname("other");
        }
        metadata.add(mapQualifier("datasourcetypeui", dsType));
    }

    private Qualifier getBestAccessright(final Result r) {
        Qualifier bestAccessRight = new Qualifier();
        bestAccessRight.setClassid("UNKNOWN");
        bestAccessRight.setClassname("not available");
        bestAccessRight.setSchemeid("dnet:access_modes");
        bestAccessRight.setSchemename("dnet:access_modes");

        final LicenseComparator lc = new LicenseComparator();
        for (final Instance instance : r.getInstance()) {
            if (lc.compare(bestAccessRight, instance.getAccessright()) > 0) {
                bestAccessRight = instance.getAccessright();
            }
        }
        return bestAccessRight;
    }

    private List<String> listRelations(final JoinedEntity je, TemplateFactory templateFactory, final Set<String> contexts) {
        final List<String> rels = Lists.newArrayList();

        for (final Tuple2 link : je.getLinks()) {

            final Relation rel = link.getRelation();
            final RelatedEntity re = link.getRelatedEntity();
            final String targetType = link.getRelatedEntity().getType();

            final List<String> metadata = Lists.newArrayList();
            switch (EntityType.valueOf(targetType)) {
                case publication:
                case dataset:
                case otherresearchproduct:
                case software:
                    if (re.getTitle() != null && isNotBlank(re.getTitle().getValue())) {
                        metadata.add(mapStructuredProperty("title", re.getTitle()));
                    }
                    if (isNotBlank(re.getDateofacceptance())) {
                        metadata.add(asXmlElement("dateofacceptance", re.getDateofacceptance()));
                    }
                    if (isNotBlank(re.getPublisher())) {
                        metadata.add(asXmlElement("publisher", re.getPublisher()));
                    }
                    if (isNotBlank(re.getCodeRepositoryUrl())) {
                        metadata.add(asXmlElement("coderepositoryurl", re.getCodeRepositoryUrl()));
                    }
                    if (re.getResulttype() != null & !re.getResulttype().isBlank()) {
                        metadata.add(mapQualifier("resulttype", re.getResulttype()));
                    }
                    if (re.getCollectedfrom() != null) {
                        metadata.addAll(re.getCollectedfrom()
                                .stream()
                                .map(kv -> mapKeyValue("collectedfrom", kv))
                                .collect(Collectors.toList()));
                    }
                    if (re.getPid() != null) {
                        metadata.addAll(re.getPid()
                                .stream()
                                .map(p -> mapStructuredProperty("pid", p))
                                .collect(Collectors.toList()));
                    }
                    break;
                case datasource:
                    if (isNotBlank(re.getOfficialname())) {
                        metadata.add(asXmlElement("officialname", re.getOfficialname()));
                    }
                    if (re.getDatasourcetype() != null & !re.getDatasourcetype().isBlank()) {
                        mapDatasourceType(metadata, re.getDatasourcetype());
                    }
                    if (re.getOpenairecompatibility() != null & !re.getOpenairecompatibility().isBlank()) {
                        metadata.add(mapQualifier("openairecompatibility", re.getOpenairecompatibility()));
                    }
                    break;
                case organization:
                    if (isNotBlank(re.getLegalname())) {
                        metadata.add(asXmlElement("legalname", re.getLegalname()));
                    }
                    if (isNotBlank(re.getLegalshortname())) {
                        metadata.add(asXmlElement("legalshortname", re.getLegalshortname()));
                    }
                    if (re.getCountry() != null & !re.getCountry().isBlank()) {
                        metadata.add(mapQualifier("country", re.getCountry()));
                    }
                    break;
                case project:
                    if (isNotBlank(re.getProjectTitle())) {
                        metadata.add(asXmlElement("title", re.getProjectTitle()));
                    }
                    if (isNotBlank(re.getCode())) {
                        metadata.add(asXmlElement("code", re.getCode()));
                    }
                    if (isNotBlank(re.getAcronym())) {
                        metadata.add(asXmlElement("acronym", re.getAcronym()));
                    }
                    if (re.getContracttype() != null & !re.getContracttype().isBlank()) {
                        metadata.add(mapQualifier("contracttype", re.getContracttype()));
                    }
                    if (re.getFundingtree() != null) {
                        metadata.addAll(re.getFundingtree()
                                .stream()
                                .peek(ft -> fillContextMap(ft, contexts))
                                .map(ft -> getRelFundingTree(ft))
                                .collect(Collectors.toList()));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("invalid target type: " + targetType);

            }
            final DataInfo info = rel.getDataInfo();
            final String scheme = getScheme(re.getType(), targetType);

            if (StringUtils.isBlank(scheme)) {
                throw new IllegalArgumentException(String.format("missing scheme for: <%s - %s>", re.getType(), targetType));
            }

            final String accumulatorName = getRelDescriptor(rel.getRelType(), rel.getSubRelType(), rel.getRelClass());
            if (accumulators.containsKey(accumulatorName)) {
                accumulators.get(accumulatorName).add(1);
            }

            rels.add(templateFactory.getRel(
                    targetType,
                    rel.getTarget(),
                    Sets.newHashSet(metadata),
                    rel.getRelClass(),
                    scheme,
                    info));
        }
        return rels;
    }

    private List<String> listChildren(final JoinedEntity je, TemplateFactory templateFactory) {

        final List<String> children = Lists.newArrayList();

        if (MainEntityType.result.toString().equals(getMainType(je.getType()))) {
            final List<Instance> instances = ((Result) je.getEntity()).getInstance();
            if (instances != null) {
                for (final Instance instance : ((Result) je.getEntity()).getInstance()) {

                    final List<String> fields = Lists.newArrayList();

                    if (instance.getAccessright() != null && !instance.getAccessright().isBlank()) {
                        fields.add(mapQualifier("accessright", instance.getAccessright()));
                    }
                    if (instance.getCollectedfrom() != null) {
                        fields.add(mapKeyValue("collectedfrom", instance.getCollectedfrom()));
                    }
                    if (instance.getHostedby() != null) {
                        fields.add(mapKeyValue("hostedby", instance.getHostedby()));
                    }
                    if (instance.getDateofacceptance() != null && isNotBlank(instance.getDateofacceptance().getValue())) {
                        fields.add(asXmlElement("dateofacceptance", instance.getDateofacceptance().getValue()));
                    }
                    if (instance.getInstancetype() != null && !instance.getInstancetype().isBlank()) {
                        fields.add(mapQualifier("instancetype", instance.getInstancetype()));
                    }
                    if (isNotBlank(instance.getDistributionlocation())) {
                        fields.add(asXmlElement("distributionlocation", instance.getDistributionlocation()));
                    }
                    if (instance.getRefereed() != null && isNotBlank(instance.getRefereed().getValue())) {
                        fields.add(asXmlElement("refereed", instance.getRefereed().getValue()));
                    }
                    if (instance.getProcessingchargeamount() != null && isNotBlank(instance.getProcessingchargeamount().getValue())) {
                        fields.add(asXmlElement("processingchargeamount", instance.getProcessingchargeamount().getValue()));
                    }
                    if (instance.getProcessingchargecurrency() != null && isNotBlank(instance.getProcessingchargecurrency().getValue())) {
                        fields.add(asXmlElement("processingchargecurrency", instance.getProcessingchargecurrency().getValue()));
                    }

                    children.add(templateFactory.getInstance(instance.getHostedby().getKey(), fields, instance.getUrl()));
                }
            }
            final List<ExternalReference> ext = ((Result) je.getEntity()).getExternalReference();
            if (ext != null) {
                for (final ExternalReference er : ((Result) je.getEntity()).getExternalReference()) {

                    final List<String> fields = Lists.newArrayList();

                    if (isNotBlank(er.getSitename())) {
                        fields.add(asXmlElement("sitename", er.getSitename()));
                    }
                    if (isNotBlank(er.getLabel())) {
                        fields.add(asXmlElement("label", er.getLabel()));
                    }
                    if (isNotBlank(er.getUrl())) {
                        fields.add(asXmlElement("url", er.getUrl()));
                    }
                    if (isNotBlank(er.getDescription())) {
                        fields.add(asXmlElement("description", er.getDescription()));
                    }
                    if (isNotBlank(er.getUrl())) {
                        fields.add(mapQualifier("qualifier", er.getQualifier()));
                    }
                    if (isNotBlank(er.getRefidentifier())) {
                        fields.add(asXmlElement("refidentifier", er.getRefidentifier()));
                    }
                    if (isNotBlank(er.getQuery())) {
                        fields.add(asXmlElement("query", er.getQuery()));
                    }

                    children.add(templateFactory.getChild("externalreference", null, fields));
                }
            }
        }

        return children;
    }

    private List<String> listExtraInfo(JoinedEntity je) {
        final List<ExtraInfo> extraInfo = je.getEntity().getExtraInfo();
        return extraInfo != null ? extraInfo
                .stream()
                .map(e -> mapExtraInfo(e))
                .collect(Collectors.toList()) : Lists.newArrayList();
    }

    private List<String> buildContexts(final String type, final Set<String> contexts) {
        final List<String> res = Lists.newArrayList();

        if ((contextMapper != null) && !contextMapper.isEmpty() && MainEntityType.result.toString().equals(type)) {

            XMLTag document = XMLDoc.newDocument(true).addRoot("contextRoot");

            for (final String context : contexts) {

                String id = "";
                for (final String token : Splitter.on("::").split(context)) {
                    id += token;

                    final ContextDef def = contextMapper.get(id);

                    if (def == null) {
                        continue;
                        // throw new IllegalStateException(String.format("cannot find context for id '%s'", id));
                    }

                    if (def.getName().equals("context")) {
                        final String xpath = "//context/@id='" + def.getId() + "'";
                        if (!document.gotoRoot().rawXpathBoolean(xpath, new Object())) {
                            document = addContextDef(document.gotoRoot(), def);
                        }
                    }

                    if (def.getName().equals("category")) {
                        final String rootId = substringBefore(def.getId(), "::");
                        document = addContextDef(document.gotoRoot().gotoTag("//context[./@id='" + rootId + "']", new Object()), def);
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

    private String asStringElement(final org.w3c.dom.Element element, final Transformer transformer) throws TransformerException {
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

                contextMapper.put(funderShortName, new ContextDef(funderShortName, funder.valueOf("./name"), "context", "funding"));
                final Node level0 = fundingPath.selectSingleNode("//funding_level_0");
                if (level0 != null) {
                    final String level0Id = Joiner.on("::").join(funderShortName, level0.valueOf("./name"));
                    contextMapper.put(level0Id, new ContextDef(level0Id, level0.valueOf("./description"), "category", ""));
                    final Node level1 = fundingPath.selectSingleNode("//funding_level_1");
                    if (level1 == null) {
                        contexts.add(level0Id);
                    } else {
                        final String level1Id = Joiner.on("::").join(level0Id, level1.valueOf("./name"));
                        contextMapper.put(level1Id, new ContextDef(level1Id, level1.valueOf("./description"), "concept", ""));
                        final Node level2 = fundingPath.selectSingleNode("//funding_level_2");
                        if (level2 == null) {
                            contexts.add(level1Id);
                        } else {
                            final String level2Id = Joiner.on("::").join(level1Id, level2.valueOf("./name"));
                            contextMapper.put(level2Id, new ContextDef(level2Id, level2.valueOf("./description"), "concept", ""));
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

            for (final Object o : Lists.reverse(ftree.selectNodes("//fundingtree//*[starts-with(local-name(),'funding_level_')]"))) {
                final Element e = (Element) o;
                final String _id = e.valueOf("./id");
                funding += "<" + e.getName() + " name=\"" + escapeXml(e.valueOf("./name")) + "\">" + escapeXml(_id) + "</" + e.getName() + ">";
            }
        } catch (final DocumentException e) {
            throw new IllegalArgumentException("unable to parse funding tree: " + xmlTree + "\n" + e.getMessage());
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

        return "<funder id=\"" + escapeXml(funderId) + "\" shortname=\"" + escapeXml(funderShortName) + "\" name=\"" + escapeXml(funderName)
                + "\" jurisdiction=\"" + escapeXml(funderJurisdiction) + "\" />";
    }

}