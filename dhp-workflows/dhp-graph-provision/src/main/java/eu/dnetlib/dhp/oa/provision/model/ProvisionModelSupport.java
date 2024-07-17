
package eu.dnetlib.dhp.oa.provision.model;

import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.solr.ExternalReference;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.jetbrains.annotations.Nullable;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.common.vocabulary.VocabularyTerm;
import eu.dnetlib.dhp.oa.provision.utils.ContextDef;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.solr.*;
import eu.dnetlib.dhp.schema.solr.AccessRight;
import eu.dnetlib.dhp.schema.solr.Author;
import eu.dnetlib.dhp.schema.solr.Context;
import eu.dnetlib.dhp.schema.solr.Country;
import eu.dnetlib.dhp.schema.solr.Datasource;
import eu.dnetlib.dhp.schema.solr.EoscIfGuidelines;
import eu.dnetlib.dhp.schema.solr.Instance;
import eu.dnetlib.dhp.schema.solr.Journal;
import eu.dnetlib.dhp.schema.solr.Measure;
import eu.dnetlib.dhp.schema.solr.OpenAccessColor;
import eu.dnetlib.dhp.schema.solr.OpenAccessRoute;
import eu.dnetlib.dhp.schema.solr.Organization;
import eu.dnetlib.dhp.schema.solr.Project;
import eu.dnetlib.dhp.schema.solr.Result;
import eu.dnetlib.dhp.schema.solr.Subject;

public class ProvisionModelSupport {

	private ProvisionModelSupport() {
	}

	public static Class[] getModelClasses() {
		List<Class<?>> modelClasses = Lists.newArrayList(ModelSupport.getOafModelClasses());
		modelClasses
			.addAll(
				Lists
					.newArrayList(
						RelatedEntityWrapper.class,
						JoinedEntity.class,
						RelatedEntity.class));
		return modelClasses.toArray(new Class[] {});
	}

	public static SolrRecord transform(JoinedEntity je, ContextMapper contextMapper, VocabularyGroup vocs) {
		SolrRecord r = new SolrRecord();
		final OafEntity e = je.getEntity();
		final RecordType type = RecordType.valueOf(e.getClass().getSimpleName().toLowerCase());
		final Boolean deletedbyinference = Optional
			.ofNullable(e.getDataInfo())
			.map(DataInfo::getDeletedbyinference)
			.orElse(null);
		r
			.setHeader(
				SolrRecordHeader
					.newInstance(
						StringUtils
							.substringAfter(
								e.getId(),
								IdentifierFactory.ID_PREFIX_SEPARATOR),
						e.getOriginalId(), type, deletedbyinference));
		r.setCollectedfrom(asProvenance(e.getCollectedfrom()));
		r.setContext(asContext(e.getContext(), contextMapper));
		r.setPid(asPid(e.getPid()));
		r.setMeasures(mapMeasures(e.getMeasures()));

		if (e instanceof eu.dnetlib.dhp.schema.oaf.Result) {
			r.setResult(mapResult((eu.dnetlib.dhp.schema.oaf.Result) e));
		} else if (e instanceof eu.dnetlib.dhp.schema.oaf.Datasource) {
			r.setDatasource(mapDatasource((eu.dnetlib.dhp.schema.oaf.Datasource) e));
		} else if (e instanceof eu.dnetlib.dhp.schema.oaf.Organization) {
			r.setOrganization(mapOrganization((eu.dnetlib.dhp.schema.oaf.Organization) e));
		} else if (e instanceof eu.dnetlib.dhp.schema.oaf.Project) {
			r.setProject(mapProject((eu.dnetlib.dhp.schema.oaf.Project) e, vocs));
		}
		r
			.setLinks(
				Optional
					.ofNullable(je.getLinks())
					.map(
						links -> links
							.stream()
							.map(rew -> mapRelatedRecord(rew, vocs))
							.collect(Collectors.toList()))
					.orElse(null));

		return r;
	}

	private static RelatedRecord mapRelatedRecord(RelatedEntityWrapper rew, VocabularyGroup vocs) {
		RelatedRecord rr = new RelatedRecord();

		final RelatedEntity re = rew.getTarget();
		final RecordType relatedRecordType = RecordType.valueOf(re.getType());
		final Relation relation = rew.getRelation();
		final String relationProvenance = Optional
			.ofNullable(relation.getDataInfo())
			.map(
				d -> Optional
					.ofNullable(d.getProvenanceaction())
					.map(Qualifier::getClassid)
					.orElse(null))
			.orElse(null);
		rr
			.setHeader(
				RelatedRecordHeader
					.newInstance(
						relation.getRelType(),
						relation.getRelClass(),
						StringUtils.substringAfter(relation.getTarget(), IdentifierFactory.ID_PREFIX_SEPARATOR),
						relatedRecordType,
						relationProvenance,
						Optional.ofNullable(relation.getDataInfo()).map(DataInfo::getTrust).orElse(null)));

		rr.setAcronym(re.getAcronym());
		rr.setCode(re.getCode());
		rr.setContracttype(mapCodeLabel(re.getContracttype()));
		rr.setCollectedfrom(asProvenance(re.getCollectedfrom()));
		rr.setCodeRepositoryUrl(re.getCodeRepositoryUrl());
		rr.setCountry(asCountry(re.getCountry()));
		rr.setDatasourcetype(mapCodeLabel(re.getDatasourcetype()));
		rr.setDatasourcetypeui(mapCodeLabel(re.getDatasourcetypeui()));
		rr.setDateofacceptance(re.getDateofacceptance());
		rr.setFunding(mapFunding(re.getFundingtree(), vocs));
		rr.setInstances(mapInstances(re.getInstances()));
		rr.setLegalname(re.getLegalname());
		rr.setLegalshortname(re.getLegalshortname());
		rr.setOfficialname(re.getOfficialname());
		rr.setOpenairecompatibility(mapCodeLabel(re.getOpenairecompatibility()));
		rr.setPid(asPid(re.getPid()));
		rr.setWebsiteurl(re.getWebsiteurl());
		rr.setProjectTitle(re.getProjectTitle());
		rr.setPublisher(re.getPublisher());
		rr.setResulttype(mapQualifier(re.getResulttype()));
		rr.setTitle(Optional.ofNullable(re.getTitle()).map(StructuredProperty::getValue).orElse(null));

		if (relation.getValidated() == null) {
			relation.setValidated(false);
		}
		if (ModelConstants.OUTCOME.equals(relation.getSubRelType())
			&& StringUtils.isNotBlank(relation.getValidationDate())) {
			rr.setValidationDate(relation.getValidationDate());
		}

		return rr;
	}

	private static Project mapProject(eu.dnetlib.dhp.schema.oaf.Project p, VocabularyGroup vocs) {
		Project ps = new Project();
		ps.setAcronym(mapField(p.getAcronym()));
		ps.setCode(mapField(p.getCode()));
		ps.setContracttype(mapCodeLabel(p.getContracttype()));
		ps.setCurrency(mapField(p.getCurrency()));
		ps.setDuration(mapField(p.getDuration()));
		ps.setOamandatepublications(mapField(p.getOamandatepublications()));
		ps.setCallidentifier(mapField(p.getCallidentifier()));
		ps.setEcarticle29_3(mapField(p.getEcarticle29_3()));
		ps.setEnddate(mapField(p.getEnddate()));
		ps.setFundedamount(p.getFundedamount());
		ps.setKeywords(mapField(p.getKeywords()));
		ps.setStartdate(mapField(p.getStartdate()));
		ps.setSubjects(asSubjectSP(p.getSubjects()));
		ps.setSummary(mapField(p.getSummary()));
		ps.setTitle(mapField(p.getTitle()));
		ps.setTotalcost(p.getTotalcost());
		ps.setWebsiteurl(mapField(p.getWebsiteurl()));
		ps.setFunding(mapFundingField(p.getFundingtree(), vocs));
		return ps;
	}

	private static Funding mapFunding(List<String> fundingtree, VocabularyGroup vocs) {
		SAXReader reader = new SAXReader();
		return Optional
			.ofNullable(fundingtree)
			.flatMap(
				ftree -> ftree
					.stream()
					.map(ft -> {
						try {
							Document doc = reader.read(new StringReader(ft));
							String countryCode = doc.valueOf("/fundingtree/funder/jurisdiction/text()");
							Country country = vocs
								.find("dnet:countries")
								.map(voc -> voc.getTerm(countryCode))
								.map(VocabularyTerm::getName)
								.map(label -> Country.newInstance(countryCode, label))
								.orElse(null);

							String level0_id = doc.valueOf("//funding_level_0/id/text()");
							String level1_id = doc.valueOf("//funding_level_1/id/text()");
							String level2_id = doc.valueOf("//funding_level_2/id/text()");

							return Funding
								.newInstance(
									Funder
										.newInstance(
											doc.valueOf("/fundingtree/funder/id/text()"),
											doc.valueOf("/fundingtree/funder/shortname/text()"),
											doc.valueOf("/fundingtree/funder/name/text()"),
											country, new ArrayList<>()),
									Optional
										.ofNullable(level0_id)
										.map(
											id -> FundingLevel
												.newInstance(
													id,
													doc.valueOf("//funding_level_0/description/text()"),
													doc.valueOf("//funding_level_0/name/text()")))
										.orElse(null),
									Optional
										.ofNullable(level1_id)
										.map(
											id -> FundingLevel
												.newInstance(
													id,
													doc.valueOf("//funding_level_1/description/text()"),
													doc.valueOf("//funding_level_1/name/text()")))
										.orElse(null),
									Optional
										.ofNullable(level2_id)
										.map(
											id -> FundingLevel
												.newInstance(
													id,
													doc.valueOf("//funding_level_2/description/text()"),
													doc.valueOf("//funding_level_2/name/text()")))
										.orElse(null));

						} catch (DocumentException e) {
							throw new IllegalArgumentException(e);
						}
					})
					.findFirst())
			.orElse(null);
	}

	private static Funding mapFundingField(List<Field<String>> fundingtree, VocabularyGroup vocs) {
		return mapFunding(
			Optional
				.ofNullable(fundingtree)
				.map(fts -> fts.stream().map(Field::getValue).collect(Collectors.toList()))
				.orElse(null),
			vocs);
	}

	private static Organization mapOrganization(eu.dnetlib.dhp.schema.oaf.Organization o) {
		Organization org = new Organization();
		org.setCountry(mapCodeLabel(o.getCountry()));
		org.setLegalname(mapField(o.getLegalname()));
		org.setLegalshortname(mapField(o.getLegalshortname()));
		org.setAlternativeNames(mapFieldList(o.getAlternativeNames()));
		org.setWebsiteurl(mapField(o.getWebsiteurl()));
		org.setLogourl(mapField(o.getLogourl()));

		org.setEcenterprise(mapField(o.getEcenterprise()));
		org.setEchighereducation(mapField(o.getEchighereducation()));
		org.setEclegalbody(mapField(o.getEclegalbody()));
		org.setEcinternationalorganization(mapField(o.getEcinternationalorganization()));
		org.setEcinternationalorganizationeurinterests(mapField(o.getEcinternationalorganizationeurinterests()));
		org.setEclegalperson(mapField(o.getEclegalperson()));
		org.setEcnonprofit(mapField(o.getEcnonprofit()));
		org.setEcnutscode(mapField(o.getEcnutscode()));
		org.setEcresearchorganization(mapField(o.getEcresearchorganization()));
		org.setEcsmevalidated(mapField(o.getEcsmevalidated()));

		return org;
	}

	private static Datasource mapDatasource(eu.dnetlib.dhp.schema.oaf.Datasource d) {
		Datasource ds = new Datasource();
		ds.setEnglishname(mapField(d.getEnglishname()));
		ds.setOfficialname(mapField(d.getOfficialname()));
		ds.setDescription(mapField(d.getDescription()));
		ds.setJournal(mapJournal(d.getJournal()));
		ds.setWebsiteurl(mapField(d.getWebsiteurl()));
		ds.setLogourl(mapField(d.getLogourl()));
		ds.setAccessinfopackage(mapFieldList(d.getAccessinfopackage()));
		ds.setCertificates(mapField(d.getCertificates()));
		ds.setCitationguidelineurl(mapField(d.getCitationguidelineurl()));
		ds.setConsenttermsofuse(d.getConsenttermsofuse());
		ds.setConsenttermsofusedate(d.getConsenttermsofusedate());
		ds.setContactemail(mapField(d.getContactemail()));
		ds.setContentpolicies(mapCodeLabel(d.getContentpolicies()));
		ds.setDatabaseaccessrestriction(mapField(d.getDatabaseaccessrestriction()));
		ds.setDatabaseaccesstype(mapField(d.getDatabaseaccesstype()));
		ds.setDataprovider(mapField(d.getDataprovider()));
		ds.setDatasourcetype(mapCodeLabel(d.getDatasourcetype()));
		ds.setDatasourcetypeui(mapCodeLabel(d.getDatasourcetypeui()));
		ds.setDatauploadrestriction(mapField(d.getDatauploadrestriction()));
		ds.setDatauploadtype(mapField(d.getDatauploadtype()));
		ds.setDateofvalidation(mapField(d.getDateofvalidation()));
		ds.setEoscdatasourcetype(mapCodeLabel(d.getEoscdatasourcetype()));
		ds.setEosctype(mapCodeLabel(d.getEosctype()));
		ds.setFulltextdownload(d.getFulltextdownload());
		ds.setJurisdiction(mapCodeLabel(d.getJurisdiction()));
		ds.setLanguages(d.getLanguages());
		ds.setLatitude(mapField(d.getLatitude()));
		ds.setLongitude(mapField(d.getLongitude()));
		ds.setLastconsenttermsofusedate(d.getLastconsenttermsofusedate());
		ds.setMissionstatementurl(mapField(d.getMissionstatementurl()));
		ds.setNamespaceprefix(mapField(d.getNamespaceprefix()));
		ds.setOdcontenttypes(mapFieldList(d.getOdcontenttypes()));
		ds.setOdlanguages(mapFieldList(d.getOdlanguages()));
		ds.setOdnumberofitems(mapField(d.getOdnumberofitems()));
		ds.setOdnumberofitemsdate(mapField(d.getOdnumberofitemsdate()));
		ds.setOdpolicies(mapField(d.getOdpolicies()));
		ds.setOpenairecompatibility(mapCodeLabel(d.getOpenairecompatibility()));
		ds.setPidsystems(mapField(d.getPidsystems()));
		ds.setPolicies(mapCodeLabelKV(d.getPolicies()));
		ds.setPreservationpolicyurl(d.getPreservationpolicyurl());
		ds.setProvidedproducttypes(ds.getProvidedproducttypes());
		ds.setReleaseenddate(mapField(d.getReleasestartdate()));
		ds.setReleasestartdate(mapField(d.getReleasestartdate()));
		ds.setResearchentitytypes(ds.getResearchentitytypes());
		ds.setResearchproductaccesspolicies(d.getResearchproductaccesspolicies());
		ds.setResearchproductmetadataaccesspolicies(d.getResearchproductmetadataaccesspolicies());
		ds.setServiceprovider(mapField(d.getServiceprovider()));
		ds.setSubjects(asSubjectSP(d.getSubjects()));
		ds.setSubmissionpolicyurl(d.getSubmissionpolicyurl());
		ds.setThematic(d.getThematic());
		ds.setContentpolicies(mapCodeLabel(d.getContentpolicies()));
		ds.setVersioncontrol(d.getVersioncontrol());
		ds.setVersioning(mapField(d.getVersioning()));

		return ds;
	}

	private static Result mapResult(eu.dnetlib.dhp.schema.oaf.Result r) {
		Result rs = new Result();

		rs.setResulttype(mapQualifier(r.getResulttype()));
		rs.setAuthor(asAuthor(r.getAuthor()));
		rs.setMaintitle(getMaintitle(r.getTitle()));
		rs.setOtherTitles(getOtherTitles(r.getTitle()));
		rs.setDescription(mapFieldList(r.getDescription()));
		rs.setSubject(asSubject(r.getSubject()));
		rs.setLanguage(asLanguage(r.getLanguage()));
		rs.setPublicationdate(mapField(r.getDateofacceptance()));
		rs.setPublisher(mapField(r.getPublisher()));
		rs.setEmbargoenddate(mapField(r.getEmbargoenddate()));
		rs.setSource(mapFieldList(r.getSource()));
		rs.setFormat(mapFieldList(r.getFormat()));
		rs.setContributor(mapFieldList(r.getContributor()));
		rs.setCoverage(mapFieldList(r.getCoverage()));
		rs
			.setBestaccessright(
				BestAccessRight
					.newInstance(r.getBestaccessright().getClassid(), r.getBestaccessright().getClassname()));
		rs.setFulltext(mapFieldList(r.getFulltext()));
		rs.setCountry(asCountry(r.getCountry()));
		rs.setEoscifguidelines(asEOSCIF(r.getEoscifguidelines()));

		rs.setIsGreen(r.getIsGreen());
		rs
			.setOpenAccessColor(
				Optional
					.ofNullable(r.getOpenAccessColor())
					.map(color -> OpenAccessColor.valueOf(color.toString()))
					.orElse(null));
		rs.setIsInDiamondJournal(r.getIsInDiamondJournal());
		rs.setPubliclyFunded(r.getPubliclyFunded());
		rs.setTransformativeAgreement(r.getTransformativeAgreement());
		rs.setExternalReference(mapExternalReference(r.getExternalReference()));
		rs.setInstance(mapInstances(r.getInstance()));

		if (r instanceof Publication) {
			Publication pub = (Publication) r;
			rs.setJournal(mapJournal(pub.getJournal()));
		} else if (r instanceof Dataset) {
			Dataset d = (Dataset) r;
			rs.setSize(mapField(d.getSize()));
			rs.setVersion(mapField(d.getVersion()));
		} else if (r instanceof Software) {
			Software sw = (Software) r;
			rs.setCodeRepositoryUrl(mapField(sw.getCodeRepositoryUrl()));
			rs.setProgrammingLanguage(mapQualifier(sw.getProgrammingLanguage()));
			rs.setDocumentationUrl(mapFieldList(sw.getDocumentationUrl()));
		} else if (r instanceof OtherResearchProduct) {
			OtherResearchProduct orp = (OtherResearchProduct) r;
			rs.setContactperson(mapFieldList(orp.getContactperson()));
			rs.setContactgroup(mapFieldList(orp.getContactgroup()));
			rs.setTool(mapFieldList(orp.getTool()));
		}
		return rs;
	}

	private static Language asLanguage(Qualifier lang) {
		return Optional
			.ofNullable(lang)
			.map(q -> Language.newInstance(q.getClassid(), q.getClassname()))
			.orElse(null);
	}

	@Nullable
	private static List<String> getOtherTitles(List<StructuredProperty> titleList) {
		return Optional
			.ofNullable(titleList)
			.map(
				titles -> titles
					.stream()
					.filter(
						t -> !"main title"
							.equals(
								Optional
									.ofNullable(t.getQualifier())
									.map(Qualifier::getClassname)
									.orElse(null)))
					.map(StructuredProperty::getValue)
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static String getMaintitle(List<StructuredProperty> titleList) {
		return Optional
			.ofNullable(titleList)
			.flatMap(
				titles -> titles
					.stream()
					.filter(
						t -> "main title"
							.equals(
								Optional
									.ofNullable(t.getQualifier())
									.map(Qualifier::getClassname)
									.orElse(null)))
					.map(StructuredProperty::getValue)
					.findFirst())
			.orElse(null);
	}

	private static List<Instance> mapInstances(List<eu.dnetlib.dhp.schema.oaf.Instance> instanceList) {
		return Optional
			.ofNullable(instanceList)
			.map(
				instances -> instances
					.stream()
					.map(instance -> {
						Instance i = new Instance();
						i.setCollectedfrom(asProvenance(instance.getCollectedfrom()));
						i.setHostedby(asProvenance(instance.getHostedby()));
						i.setFulltext(instance.getFulltext());
						i.setPid(asPid(instance.getPid()));
						i.setAlternateIdentifier(asPid(instance.getAlternateIdentifier()));
						i.setAccessright(mapAccessRight(instance.getAccessright()));
						i.setInstancetype(mapQualifier(instance.getInstancetype()));
						i.setLicense(mapField(instance.getLicense()));
						i.setUrl(instance.getUrl());
						i.setRefereed(mapQualifier(instance.getRefereed()));
						i.setDateofacceptance(mapField(instance.getDateofacceptance()));
						i.setDistributionlocation(instance.getDistributionlocation());
						i.setProcessingcharges(getProcessingcharges(instance));
						return i;
					})
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static APC getProcessingcharges(eu.dnetlib.dhp.schema.oaf.Instance instance) {
		return Optional
			.of(
				APC
					.newInstance(
						mapField(instance.getProcessingchargecurrency()),
						mapField(instance.getProcessingchargeamount())))
			.filter(apc -> Objects.nonNull(apc.getAmount()) && Objects.nonNull(apc.getCurrency()))
			.orElse(null);
	}

	private static AccessRight mapAccessRight(eu.dnetlib.dhp.schema.oaf.AccessRight accessright) {
		return AccessRight
			.newInstance(
				accessright.getClassid(),
				accessright.getClassname(),
				Optional
					.ofNullable(accessright.getOpenAccessRoute())
					.map(route -> OpenAccessRoute.valueOf(route.toString()))
					.orElse(null));
	}

	private static <T> T mapField(eu.dnetlib.dhp.schema.oaf.Field<T> f) {
		return Optional.ofNullable(f).map(Field::getValue).orElse(null);
	}

	private static <T> List<T> mapFieldList(List<eu.dnetlib.dhp.schema.oaf.Field<T>> fl) {
		return Optional
			.ofNullable(fl)
			.map(v -> v.stream().map(Field::getValue).collect(Collectors.toList()))
			.orElse(null);
	}

	private static String mapQualifier(eu.dnetlib.dhp.schema.oaf.Qualifier q) {
		return Optional.ofNullable(q).map(Qualifier::getClassname).orElse(null);
	}

	private static Journal mapJournal(eu.dnetlib.dhp.schema.oaf.Journal joaf) {
		return Optional
			.ofNullable(joaf)
			.map(jo -> {
				Journal j = new Journal();
				j.setConferencedate(jo.getConferencedate());
				j.setConferenceplace(jo.getConferenceplace());
				j.setEdition(jo.getEdition());
				j.setSp(jo.getSp());
				j.setEp(jo.getEp());
				j.setVol(jo.getVol());
				j.setIss(jo.getEdition());
				j.setName(jo.getName());
				j.setIssnPrinted(jo.getIssnPrinted());
				j.setIssnOnline(jo.getIssnOnline());
				j.setIssnLinking(jo.getIssnLinking());
				return j;
			})
			.orElse(null);
	}

	private static List<Provenance> asProvenance(List<KeyValue> keyValueList) {
		return Optional
			.ofNullable(keyValueList)
			.map(
				kvs -> kvs
					.stream()
					.map(ProvisionModelSupport::asProvenance)
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static Provenance asProvenance(KeyValue keyValue) {
		return Optional
			.ofNullable(keyValue)
			.map(
				kv -> Provenance
					.newInstance(
						StringUtils.substringAfter(kv.getKey(), IdentifierFactory.ID_PREFIX_SEPARATOR),
						kv.getValue()))
			.orElse(null);
	}

	private static List<Measure> mapMeasures(List<eu.dnetlib.dhp.schema.oaf.Measure> measures) {
		return Optional
			.ofNullable(measures)
			.map(
				ml -> ml
					.stream()
					.map(m -> Measure.newInstance(m.getId(), mapCodeLabelKV(m.getUnit())))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<ExternalReference> mapExternalReference(List<eu.dnetlib.dhp.schema.oaf.ExternalReference> externalReference) {
		return Optional.ofNullable(externalReference)
				.map(ext -> ext.stream()
						.map(e -> ExternalReference.newInstance(
								e.getSitename(),
								e.getLabel(),
								e.getAlternateLabel(),
								e.getUrl(),
								mapCodeLabel(e.getQualifier()),
								e.getRefidentifier(),
								e.getQuery()))
						.collect(Collectors.toList()))
				.orElse(Lists.newArrayList());
	}

	private static List<Context> asContext(List<eu.dnetlib.dhp.schema.oaf.Context> ctxList,
		ContextMapper contextMapper) {

		final Set<String> contexts = Optional
			.ofNullable(ctxList)
			.map(
				ctx -> ctx
					.stream()
					.map(eu.dnetlib.dhp.schema.oaf.Context::getId)
					.collect(Collectors.toCollection(HashSet::new)))
			.orElse(new HashSet<>());

		/* FIXME: Workaround for CLARIN mining issue: #3670#note-29 */
		if (contexts.contains("dh-ch::subcommunity::2")) {
			contexts.add("clarin");
		}

		return Optional
			.of(contexts)
			.map(
				ctx -> ctx
					.stream()
					.map(contextPath -> {
						Context context = new Context();
						String id = "";
						Map<String, Category> categoryMap = Maps.newHashMap();
						for (final String token : Splitter.on("::").split(contextPath)) {
							id += token;

							final ContextDef def = contextMapper.get(id);

							if (def == null) {
								continue;
							}
							if (def.getName().equals("context")) {
								context.setId(def.getId());
								context.setLabel(def.getLabel());
								context.setType(def.getType());
							}
							if (def.getName().equals("category")) {
								Category category = Category.newInstance(def.getId(), def.getLabel());
								if (Objects.isNull(context.getCategory())) {
									context.setCategory(Lists.newArrayList());
								}
								context.getCategory().add(category);
								categoryMap.put(def.getId(), category);
							}
							if (def.getName().equals("concept")) {
								String parentId = StringUtils.substringBeforeLast(def.getId(), "::");
								if (categoryMap.containsKey(parentId)) {
									categoryMap
										.get(parentId)
										.getConcept()
										.add(Concept.newInstance(def.getId(), def.getLabel()));
								}
							}
							id += "::";
						}
						return context;
					})
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<Pid> asPid(List<StructuredProperty> pidList) {
		return Optional
			.ofNullable(pidList)
			.map(
				pids -> pids
					.stream()
					.filter(p -> Objects.nonNull(p.getQualifier()))
					.filter(p -> Objects.nonNull(p.getQualifier().getClassid()))
					.map(
						p -> Pid
							.newInstance(
								p.getValue(),
								p.getQualifier().getClassid(),
								p.getQualifier().getClassname()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<Author> asAuthor(List<eu.dnetlib.dhp.schema.oaf.Author> authorList) {
		return Optional
			.ofNullable(authorList)
			.map(
				authors -> authors
					.stream()
					.map(
						a -> Author
							.newInstance(a.getFullname(), a.getName(), a.getSurname(), a.getRank(), asPid(a.getPid())))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<Subject> asSubject(List<eu.dnetlib.dhp.schema.oaf.Subject> subjectList) {
		return Optional
			.ofNullable(subjectList)
			.map(
				subjects -> subjects
					.stream()
					.filter(s -> Objects.nonNull(s.getQualifier()))
					.filter(s -> Objects.nonNull(s.getQualifier().getClassname()))
					.map(
						s -> Subject
							.newInstance(s.getValue(), s.getQualifier().getClassid(), s.getQualifier().getClassname()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<Subject> asSubjectSP(List<eu.dnetlib.dhp.schema.oaf.StructuredProperty> subjectList) {
		return Optional
			.ofNullable(subjectList)
			.map(
				subjects -> subjects
					.stream()
					.filter(s -> Objects.nonNull(s.getQualifier()))
					.filter(s -> Objects.nonNull(s.getQualifier().getClassname()))
					.map(
						s -> Subject
							.newInstance(s.getValue(), s.getQualifier().getClassid(), s.getQualifier().getClassname()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static Country asCountry(eu.dnetlib.dhp.schema.oaf.Qualifier country) {
		return Optional
			.ofNullable(country)
			.filter(c -> Objects.nonNull(c.getClassid()) && Objects.nonNull(c.getClassname()))
			.map(c -> Country.newInstance(c.getClassid(), c.getClassname()))
			.orElse(null);
	}

	private static List<Country> asCountry(List<eu.dnetlib.dhp.schema.oaf.Country> countryList) {
		return Optional
			.ofNullable(countryList)
			.map(
				countries -> countries
					.stream()
					.map(c -> Country.newInstance(c.getClassid(), c.getClassname()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<EoscIfGuidelines> asEOSCIF(List<eu.dnetlib.dhp.schema.oaf.EoscIfGuidelines> eoscIfGuidelines) {
		return Optional
			.ofNullable(eoscIfGuidelines)
			.map(
				eoscif -> eoscif
					.stream()
					.map(
						e -> EoscIfGuidelines
							.newInstance(e.getCode(), e.getLabel(), e.getUrl(), e.getSemanticRelation()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<CodeLabel> mapCodeLabelKV(List<KeyValue> kvList) {
		return Optional
			.ofNullable(kvList)
			.map(
				kvs -> kvs
					.stream()
					.map(ProvisionModelSupport::mapCodeLabel)
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static List<CodeLabel> mapCodeLabel(List<Qualifier> qualifiers) {
		return Optional
			.ofNullable(qualifiers)
			.map(
				list -> list
					.stream()
					.map(ProvisionModelSupport::mapCodeLabel)
					.collect(Collectors.toList()))
			.orElse(null);
	}

	private static CodeLabel mapCodeLabel(Qualifier qualifier) {
		return Optional
			.ofNullable(qualifier)
			.map(q -> CodeLabel.newInstance(q.getClassid(), q.getClassname()))
			.orElse(null);
	}

	private static CodeLabel mapCodeLabel(KeyValue kv) {
		return Optional
			.ofNullable(kv)
			.map(k -> CodeLabel.newInstance(k.getKey(), k.getValue()))
			.orElse(null);
	}

}
