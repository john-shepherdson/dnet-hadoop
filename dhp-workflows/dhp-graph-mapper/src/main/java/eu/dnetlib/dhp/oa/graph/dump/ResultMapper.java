
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.oa.graph.dump.exceptions.NoAvailableEntityTypeException;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.*;
import eu.dnetlib.dhp.schema.dump.oaf.AccessRight;
import eu.dnetlib.dhp.schema.dump.oaf.Author;
import eu.dnetlib.dhp.schema.dump.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.dump.oaf.Instance;
import eu.dnetlib.dhp.schema.dump.oaf.Measure;
import eu.dnetlib.dhp.schema.dump.oaf.OpenAccessRoute;
import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.schema.dump.oaf.community.CfHbKeyValue;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityInstance;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Context;
import eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult;
import eu.dnetlib.dhp.schema.oaf.*;

public class ResultMapper implements Serializable {

	public static <E extends eu.dnetlib.dhp.schema.oaf.OafEntity> Result map(
		E in, Map<String, String> communityMap, String dumpType) throws NoAvailableEntityTypeException {

		Result out;
		if (Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
			out = new GraphResult();
		} else {
			out = new CommunityResult();
		}

		eu.dnetlib.dhp.schema.oaf.Result input = (eu.dnetlib.dhp.schema.oaf.Result) in;
		Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> ort = Optional.ofNullable(input.getResulttype());
		if (ort.isPresent()) {
			try {

				addTypeSpecificInformation(out, input, ort);

				Optional
					.ofNullable(input.getAuthor())
					.ifPresent(
						ats -> out.setAuthor(ats.stream().map(ResultMapper::getAuthor).collect(Collectors.toList())));

				// I do not map Access Right UNKNOWN or OTHER

				Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> oar = Optional.ofNullable(input.getBestaccessright());
				if (oar.isPresent() && Constants.accessRightsCoarMap.containsKey(oar.get().getClassid())) {
					String code = Constants.accessRightsCoarMap.get(oar.get().getClassid());
					out
						.setBestaccessright(

							BestAccessRight
								.newInstance(
									code,
									Constants.coarCodeLabelMap.get(code),
									Constants.COAR_ACCESS_RIGHT_SCHEMA));
				}

				final List<String> contributorList = new ArrayList<>();
				Optional
					.ofNullable(input.getContributor())
					.ifPresent(value -> value.stream().forEach(c -> contributorList.add(c.getValue())));
				out.setContributor(contributorList);

				Optional
					.ofNullable(input.getCountry())
					.ifPresent(
						value -> out
							.setCountry(
								value
									.stream()
									.map(
										c -> {
											if (c.getClassid().equals((ModelConstants.UNKNOWN))) {
												return null;
											}
											ResultCountry country = new ResultCountry();
											country.setCode(c.getClassid());
											country.setLabel(c.getClassname());
											Optional
												.ofNullable(c.getDataInfo())
												.ifPresent(
													provenance -> country
														.setProvenance(
															Provenance
																.newInstance(
																	provenance
																		.getProvenanceaction()
																		.getClassname(),
																	c.getDataInfo().getTrust())));
											return country;
										})
									.filter(Objects::nonNull)
									.collect(Collectors.toList())));

				final List<String> coverageList = new ArrayList<>();
				Optional
					.ofNullable(input.getCoverage())
					.ifPresent(value -> value.stream().forEach(c -> coverageList.add(c.getValue())));
				out.setCoverage(coverageList);

				out.setDateofcollection(input.getDateofcollection());

				final List<String> descriptionList = new ArrayList<>();
				Optional
					.ofNullable(input.getDescription())
					.ifPresent(value -> value.forEach(d -> descriptionList.add(d.getValue())));
				out.setDescription(descriptionList);
				Optional<Field<String>> oStr = Optional.ofNullable(input.getEmbargoenddate());
				if (oStr.isPresent()) {
					out.setEmbargoenddate(oStr.get().getValue());
				}

				final List<String> formatList = new ArrayList<>();
				Optional
					.ofNullable(input.getFormat())
					.ifPresent(value -> value.stream().forEach(f -> formatList.add(f.getValue())));
				out.setFormat(formatList);
				out.setId(input.getId());
				out.setOriginalId(new ArrayList<>());
				Optional
					.ofNullable(input.getOriginalId())
					.ifPresent(
						v -> out
							.setOriginalId(
								input
									.getOriginalId()
									.stream()
									.filter(s -> !s.startsWith("50|"))
									.collect(Collectors.toList())));

				Optional<List<eu.dnetlib.dhp.schema.oaf.Instance>> oInst = Optional
					.ofNullable(input.getInstance());

				if (oInst.isPresent()) {
					if (Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
						((GraphResult) out)
							.setInstance(
								oInst.get().stream().map(ResultMapper::getGraphInstance).collect(Collectors.toList()));
					} else {
						((CommunityResult) out)
							.setInstance(
								oInst
									.get()
									.stream()
									.map(ResultMapper::getCommunityInstance)
									.collect(Collectors.toList()));
					}
				}

				Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> oL = Optional.ofNullable(input.getLanguage());
				if (oL.isPresent()) {
					eu.dnetlib.dhp.schema.oaf.Qualifier language = oL.get();
					out.setLanguage(Language.newInstance(language.getClassid(), language.getClassname()));
				}
				Optional<Long> oLong = Optional.ofNullable(input.getLastupdatetimestamp());
				if (oLong.isPresent()) {
					out.setLastupdatetimestamp(oLong.get());
				}
				Optional<List<StructuredProperty>> otitle = Optional.ofNullable(input.getTitle());
				if (otitle.isPresent()) {
					List<StructuredProperty> iTitle = otitle
						.get()
						.stream()
						.filter(t -> t.getQualifier().getClassid().equalsIgnoreCase("main title"))
						.collect(Collectors.toList());
					if (!iTitle.isEmpty()) {
						out.setMaintitle(iTitle.get(0).getValue());
					}

					iTitle = otitle
						.get()
						.stream()
						.filter(t -> t.getQualifier().getClassid().equalsIgnoreCase("subtitle"))
						.collect(Collectors.toList());
					if (!iTitle.isEmpty()) {
						out.setSubtitle(iTitle.get(0).getValue());
					}

				}

				Optional
					.ofNullable(input.getPid())
					.ifPresent(
						value -> out
							.setPid(
								value
									.stream()
									.map(
										p -> ResultPid
											.newInstance(p.getQualifier().getClassid(), p.getValue()))
									.collect(Collectors.toList())));

				oStr = Optional.ofNullable(input.getDateofacceptance());
				if (oStr.isPresent()) {
					out.setPublicationdate(oStr.get().getValue());
				}
				oStr = Optional.ofNullable(input.getPublisher());
				if (oStr.isPresent()) {
					out.setPublisher(oStr.get().getValue());
				}

				Optional
					.ofNullable(input.getSource())
					.ifPresent(
						value -> out.setSource(value.stream().map(Field::getValue).collect(Collectors.toList())));

				List<Subject> subjectList = new ArrayList<>();
				Optional
					.ofNullable(input.getSubject())
					.ifPresent(
						value -> value
							.forEach(s -> subjectList.add(getSubject(s))));

				out.setSubjects(subjectList);

				out.setType(input.getResulttype().getClassid());

				if (!Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
					((CommunityResult) out)
						.setCollectedfrom(
							input
								.getCollectedfrom()
								.stream()
								.map(cf -> CfHbKeyValue.newInstance(cf.getKey(), cf.getValue()))
								.collect(Collectors.toList()));

					Set<String> communities = communityMap.keySet();
					List<Context> contextList = Optional
						.ofNullable(
							input
								.getContext())
						.map(
							value -> value
								.stream()
								.map(c -> {
									String communityId = c.getId();
									if (communityId.contains("::")) {
										communityId = communityId.substring(0, communityId.indexOf("::"));
									}
									if (communities.contains(communityId)) {
										Context context = new Context();
										context.setCode(communityId);
										context.setLabel(communityMap.get(communityId));
										Optional<List<DataInfo>> dataInfo = Optional.ofNullable(c.getDataInfo());
										if (dataInfo.isPresent()) {
											List<Provenance> provenance = new ArrayList<>();
											provenance
												.addAll(
													dataInfo
														.get()
														.stream()
														.map(
															di -> Optional
																.ofNullable(di.getProvenanceaction())
																.map(
																	provenanceaction -> Provenance
																		.newInstance(
																			provenanceaction.getClassname(),
																			di.getTrust()))
																.orElse(null))
														.filter(Objects::nonNull)
														.collect(Collectors.toSet()));

											try {
												context.setProvenance(getUniqueProvenance(provenance));
											} catch (NoAvailableEntityTypeException e) {
												e.printStackTrace();
											}
										}
										return context;
									}
									return null;
								})
								.filter(Objects::nonNull)
								.collect(Collectors.toList()))
						.orElse(new ArrayList<>());

					if (!contextList.isEmpty()) {
						Set<Integer> hashValue = new HashSet<>();
						List<Context> remainigContext = new ArrayList<>();
						contextList.forEach(c -> {
							if (!hashValue.contains(c.hashCode())) {
								remainigContext.add(c);
								hashValue.add(c.hashCode());
							}
						});
						((CommunityResult) out).setContext(remainigContext);
					}
				}
			} catch (ClassCastException cce) {
				return out;
			}
		}

		return out;

	}

	private static void addTypeSpecificInformation(Result out, eu.dnetlib.dhp.schema.oaf.Result input,
		Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> ort) throws NoAvailableEntityTypeException {
		switch (ort.get().getClassid()) {
			case "publication":
				Optional<Journal> journal = Optional
					.ofNullable(((Publication) input).getJournal());
				if (journal.isPresent()) {
					Journal j = journal.get();
					Container c = new Container();
					c.setConferencedate(j.getConferencedate());
					c.setConferenceplace(j.getConferenceplace());
					c.setEdition(j.getEdition());
					c.setEp(j.getEp());
					c.setIss(j.getIss());
					c.setIssnLinking(j.getIssnLinking());
					c.setIssnOnline(j.getIssnOnline());
					c.setIssnPrinted(j.getIssnPrinted());
					c.setName(j.getName());
					c.setSp(j.getSp());
					c.setVol(j.getVol());
					out.setContainer(c);
					out.setType(ModelConstants.PUBLICATION_DEFAULT_RESULTTYPE.getClassname());
				}
				break;
			case "dataset":
				Dataset id = (Dataset) input;
				Optional.ofNullable(id.getSize()).ifPresent(v -> out.setSize(v.getValue()));
				Optional.ofNullable(id.getVersion()).ifPresent(v -> out.setVersion(v.getValue()));

				out
					.setGeolocation(
						Optional
							.ofNullable(id.getGeolocation())
							.map(
								igl -> igl
									.stream()
									.filter(Objects::nonNull)
									.map(gli -> {
										GeoLocation gl = new GeoLocation();
										gl.setBox(gli.getBox());
										gl.setPlace(gli.getPlace());
										gl.setPoint(gli.getPoint());
										return gl;
									})
									.collect(Collectors.toList()))
							.orElse(null));

				out.setType(ModelConstants.DATASET_DEFAULT_RESULTTYPE.getClassname());
				break;
			case "software":

				Software is = (Software) input;
				Optional
					.ofNullable(is.getCodeRepositoryUrl())
					.ifPresent(value -> out.setCodeRepositoryUrl(value.getValue()));
				Optional
					.ofNullable(is.getDocumentationUrl())
					.ifPresent(
						value -> out
							.setDocumentationUrl(
								value
									.stream()
									.map(Field::getValue)
									.collect(Collectors.toList())));

				Optional
					.ofNullable(is.getProgrammingLanguage())
					.ifPresent(value -> out.setProgrammingLanguage(value.getClassid()));

				out.setType(ModelConstants.SOFTWARE_DEFAULT_RESULTTYPE.getClassname());
				break;
			case "other":

				OtherResearchProduct ir = (OtherResearchProduct) input;
				out
					.setContactgroup(
						Optional
							.ofNullable(ir.getContactgroup())
							.map(value -> value.stream().map(Field::getValue).collect(Collectors.toList()))
							.orElse(null));

				out
					.setContactperson(
						Optional
							.ofNullable(ir.getContactperson())
							.map(value -> value.stream().map(Field::getValue).collect(Collectors.toList()))
							.orElse(null));
				out
					.setTool(
						Optional
							.ofNullable(ir.getTool())
							.map(value -> value.stream().map(Field::getValue).collect(Collectors.toList()))
							.orElse(null));

				out.setType(ModelConstants.ORP_DEFAULT_RESULTTYPE.getClassname());

				break;
			default:
				throw new NoAvailableEntityTypeException();
		}
	}

	private static Instance getGraphInstance(eu.dnetlib.dhp.schema.oaf.Instance i) {
		Instance instance = new Instance();

		setCommonValue(i, instance);

		return instance;

	}

	private static CommunityInstance getCommunityInstance(eu.dnetlib.dhp.schema.oaf.Instance i) {
		CommunityInstance instance = new CommunityInstance();

		setCommonValue(i, instance);

		instance
			.setCollectedfrom(
				CfHbKeyValue
					.newInstance(i.getCollectedfrom().getKey(), i.getCollectedfrom().getValue()));

		instance
			.setHostedby(
				CfHbKeyValue.newInstance(i.getHostedby().getKey(), i.getHostedby().getValue()));

		return instance;

	}

	private static <I extends Instance> void setCommonValue(eu.dnetlib.dhp.schema.oaf.Instance i, I instance) {
		Optional<eu.dnetlib.dhp.schema.oaf.AccessRight> opAr = Optional.ofNullable(i.getAccessright());

		if (opAr.isPresent() && Constants.accessRightsCoarMap.containsKey(opAr.get().getClassid())) {
			String code = Constants.accessRightsCoarMap.get(opAr.get().getClassid());

			instance
				.setAccessright(
					AccessRight
						.newInstance(
							code,
							Constants.coarCodeLabelMap.get(code),
							Constants.COAR_ACCESS_RIGHT_SCHEMA));

			Optional<List<eu.dnetlib.dhp.schema.oaf.Measure>> mes = Optional.ofNullable(i.getMeasures());
			if (mes.isPresent()) {
				List<Measure> measure = new ArrayList<>();
				mes
					.get()
					.forEach(
						m -> m.getUnit().forEach(u -> measure.add(Measure.newInstance(m.getId(), u.getValue()))));
				instance.setMeasures(measure);
			}

			if (opAr.get().getOpenAccessRoute() != null) {
				switch (opAr.get().getOpenAccessRoute()) {
					case hybrid:
						instance.getAccessright().setOpenAccessRoute(OpenAccessRoute.hybrid);
						break;
					case gold:
						instance.getAccessright().setOpenAccessRoute(OpenAccessRoute.gold);
						break;
					case green:
						instance.getAccessright().setOpenAccessRoute(OpenAccessRoute.green);
						break;
					case bronze:
						instance.getAccessright().setOpenAccessRoute(OpenAccessRoute.bronze);
						break;

				}
			}

		}

		Optional
			.ofNullable(i.getPid())
			.ifPresent(
				pid -> instance
					.setPid(
						pid
							.stream()
							.map(p -> ResultPid.newInstance(p.getQualifier().getClassid(), p.getValue()))
							.collect(Collectors.toList())));

		Optional
			.ofNullable(i.getAlternateIdentifier())
			.ifPresent(
				ai -> instance
					.setAlternateIdentifier(
						ai
							.stream()
							.map(p -> AlternateIdentifier.newInstance(p.getQualifier().getClassid(), p.getValue()))
							.collect(Collectors.toList())));

		Optional
			.ofNullable(i.getLicense())
			.ifPresent(value -> instance.setLicense(value.getValue()));
		Optional
			.ofNullable(i.getDateofacceptance())
			.ifPresent(value -> instance.setPublicationdate(value.getValue()));
		Optional
			.ofNullable(i.getRefereed())
			.ifPresent(value -> instance.setRefereed(value.getClassname()));
		Optional
			.ofNullable(i.getInstancetype())
			.ifPresent(value -> instance.setType(value.getClassname()));
		Optional.ofNullable(i.getUrl()).ifPresent(value -> instance.setUrl(value));
		Optional<Field<String>> oPca = Optional.ofNullable(i.getProcessingchargeamount());
		Optional<Field<String>> oPcc = Optional.ofNullable(i.getProcessingchargecurrency());
		if (oPca.isPresent() && oPcc.isPresent()) {
			Field<String> pca = oPca.get();
			Field<String> pcc = oPcc.get();
			if (!pca.getValue().trim().equals("") && !pcc.getValue().trim().equals("")) {
				APC apc = new APC();
				apc.setCurrency(oPcc.get().getValue());
				apc.setAmount(oPca.get().getValue());
				instance.setArticleprocessingcharge(apc);
			}

		}
		Optional.ofNullable(i.getUrl()).ifPresent(instance::setUrl);

	}

	private static List<Provenance> getUniqueProvenance(List<Provenance> provenance)
		throws NoAvailableEntityTypeException {
		Provenance iProv = new Provenance();

		Provenance hProv = new Provenance();
		Provenance lProv = new Provenance();

		for (Provenance p : provenance) {
			switch (p.getProvenance()) {
				case Constants.HARVESTED:
					hProv = getHighestTrust(hProv, p);
					break;
				case Constants.INFERRED:
					iProv = getHighestTrust(iProv, p);
					// To be removed as soon as the new beta run has been done
					// this fixex issue of not set trust during bulktagging
					if (StringUtils.isEmpty(iProv.getTrust())) {
						iProv.setTrust(Constants.DEFAULT_TRUST);
					}
					break;
				case Constants.USER_CLAIM:
					lProv = getHighestTrust(lProv, p);
					break;
				default:
					throw new NoAvailableEntityTypeException();
			}

		}

		return Arrays
			.asList(iProv, hProv, lProv)
			.stream()
			.filter(p -> !StringUtils.isEmpty(p.getProvenance()))
			.collect(Collectors.toList());

	}

	private static Provenance getHighestTrust(Provenance hProv, Provenance p) {
		if (StringUtils.isNoneEmpty(hProv.getTrust(), p.getTrust()))
			return hProv.getTrust().compareTo(p.getTrust()) > 0 ? hProv : p;

		return (StringUtils.isEmpty(p.getTrust()) && !StringUtils.isEmpty(hProv.getTrust())) ? hProv : p;

	}

	private static Subject getSubject(StructuredProperty s) {
		Subject subject = new Subject();
		subject.setSubject(SubjectSchemeValue.newInstance(s.getQualifier().getClassid(), s.getValue()));
		Optional<DataInfo> di = Optional.ofNullable(s.getDataInfo());
		if (di.isPresent()) {
			Provenance p = new Provenance();
			p.setProvenance(di.get().getProvenanceaction().getClassname());
			p.setTrust(di.get().getTrust());
			subject.setProvenance(p);
		}

		return subject;
	}

	private static Author getAuthor(eu.dnetlib.dhp.schema.oaf.Author oa) {
		Author a = new Author();
		a.setFullname(oa.getFullname());
		a.setName(oa.getName());
		a.setSurname(oa.getSurname());
		a.setRank(oa.getRank());

		Optional<List<StructuredProperty>> oPids = Optional
			.ofNullable(oa.getPid());
		if (oPids.isPresent()) {
			AuthorPid pid = getOrcid(oPids.get());
			if (pid != null) {
				a.setPid(pid);
			}
		}

		return a;
	}

	private static AuthorPid getAuthorPid(StructuredProperty pid) {
		Optional<DataInfo> di = Optional.ofNullable(pid.getDataInfo());
		if (di.isPresent()) {
			return AuthorPid
				.newInstance(
					AuthorPidSchemeValue
						.newInstance(
							pid.getQualifier().getClassid(),
							pid.getValue()),
					Provenance
						.newInstance(
							di.get().getProvenanceaction().getClassname(),
							di.get().getTrust()));
		} else {
			return AuthorPid
				.newInstance(
					AuthorPidSchemeValue
						.newInstance(
							pid.getQualifier().getClassid(),
							pid.getValue())

				);
		}
	}

	private static AuthorPid getOrcid(List<StructuredProperty> p) {
		List<StructuredProperty> pidList = p.stream().map(pid -> {
			if (pid.getQualifier().getClassid().equals(ModelConstants.ORCID) ||
				(pid.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING))) {
				return pid;
			}
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());

		if (pidList.size() == 1) {
			return getAuthorPid(pidList.get(0));
		}

		List<StructuredProperty> orcid = pidList
			.stream()
			.filter(
				ap -> ap
					.getQualifier()
					.getClassid()
					.equals(ModelConstants.ORCID))
			.collect(Collectors.toList());
		if (orcid.size() == 1) {
			return getAuthorPid(orcid.get(0));
		}
		orcid = pidList
			.stream()
			.filter(
				ap -> ap
					.getQualifier()
					.getClassid()
					.equals(ModelConstants.ORCID_PENDING))
			.collect(Collectors.toList());
		if (orcid.size() == 1) {
			return getAuthorPid(orcid.get(0));
		}

		return null;
	}

}
