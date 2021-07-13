
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.*;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityInstance;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Context;
import eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ResultMapper implements Serializable {

	public static <E extends eu.dnetlib.dhp.schema.oaf.OafEntity> Result map(
		E in, Map<String, String> communityMap, String dumpType) {

		Result out;
		if (Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
			out = new GraphResult();
		} else {
			out = new CommunityResult();
		}

		eu.dnetlib.dhp.schema.oaf.Result input = (eu.dnetlib.dhp.schema.oaf.Result) in;
		Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> ort = Optional.ofNullable(input.getResulttype());
		if (ort.isPresent()) {
			switch (ort.get().getClassid()) {
				case "publication":
					Optional<Journal> journal = Optional
						.ofNullable(((eu.dnetlib.dhp.schema.oaf.Publication) input).getJournal());
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
					eu.dnetlib.dhp.schema.oaf.Dataset id = (eu.dnetlib.dhp.schema.oaf.Dataset) input;
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

					eu.dnetlib.dhp.schema.oaf.Software is = (eu.dnetlib.dhp.schema.oaf.Software) input;
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
										.map(v -> v.getValue())
										.collect(Collectors.toList())));

					Optional
						.ofNullable(is.getProgrammingLanguage())
						.ifPresent(value -> out.setProgrammingLanguage(value.getClassid()));

					out.setType(ModelConstants.SOFTWARE_DEFAULT_RESULTTYPE.getClassname());
					break;
				case "other":

					eu.dnetlib.dhp.schema.oaf.OtherResearchProduct ir = (eu.dnetlib.dhp.schema.oaf.OtherResearchProduct) input;
					out
						.setContactgroup(
							Optional
								.ofNullable(ir.getContactgroup())
								.map(value -> value.stream().map(cg -> cg.getValue()).collect(Collectors.toList()))
								.orElse(null));

					out
						.setContactperson(
							Optional
								.ofNullable(ir.getContactperson())
								.map(value -> value.stream().map(cp -> cp.getValue()).collect(Collectors.toList()))
								.orElse(null));
					out
						.setTool(
							Optional
								.ofNullable(ir.getTool())
								.map(value -> value.stream().map(t -> t.getValue()).collect(Collectors.toList()))
								.orElse(null));

					out.setType(ModelConstants.ORP_DEFAULT_RESULTTYPE.getClassname());

					break;
			}

			Optional
				.ofNullable(input.getAuthor())
				.ifPresent(ats -> out.setAuthor(ats.stream().map(at -> getAuthor(at)).collect(Collectors.toList())));

			// I do not map Access Right UNKNOWN or OTHER

			Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> oar = Optional.ofNullable(input.getBestaccessright());
			if (oar.isPresent()) {
				if (Constants.accessRightsCoarMap.containsKey(oar.get().getClassid())) {
					String code = Constants.accessRightsCoarMap.get(oar.get().getClassid());
					out
						.setBestaccessright(
							AccessRight
								.newInstance(
									code,
									Constants.coarCodeLabelMap.get(code),
									Constants.COAR_ACCESS_RIGHT_SCHEMA));
				}
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
										Country country = new Country();
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
			out.setOriginalId(input.getOriginalId());

			Optional<List<eu.dnetlib.dhp.schema.oaf.Instance>> oInst = Optional
				.ofNullable(input.getInstance());

			if (oInst.isPresent()) {
				if (Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
					((GraphResult) out)
						.setInstance(oInst.get().stream().map(i -> getGraphInstance(i)).collect(Collectors.toList()));
				} else {
					((CommunityResult) out)
						.setInstance(
							oInst.get().stream().map(i -> getCommunityInstance(i)).collect(Collectors.toList()));
				}
			}

			Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> oL = Optional.ofNullable(input.getLanguage());
			if (oL.isPresent()) {
				eu.dnetlib.dhp.schema.oaf.Qualifier language = oL.get();
				out.setLanguage(Qualifier.newInstance(language.getClassid(), language.getClassname()));
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
				if (iTitle.size() > 0) {
					out.setMaintitle(iTitle.get(0).getValue());
				}

				iTitle = otitle
					.get()
					.stream()
					.filter(t -> t.getQualifier().getClassid().equalsIgnoreCase("subtitle"))
					.collect(Collectors.toList());
				if (iTitle.size() > 0) {
					out.setSubtitle(iTitle.get(0).getValue());
				}

			}

			List<ControlledField> pids = new ArrayList<>();
			Optional
				.ofNullable(input.getPid())
				.ifPresent(
					value -> value
						.stream()
						.forEach(
							p -> pids
								.add(
									ControlledField
										.newInstance(p.getQualifier().getClassid(), p.getValue()))));
			out.setPid(pids);
			oStr = Optional.ofNullable(input.getDateofacceptance());
			if (oStr.isPresent()) {
				out.setPublicationdate(oStr.get().getValue());
			}
			oStr = Optional.ofNullable(input.getPublisher());
			if (oStr.isPresent()) {
				out.setPublisher(oStr.get().getValue());
			}

			List<String> sourceList = new ArrayList<>();
			Optional
				.ofNullable(input.getSource())
				.ifPresent(value -> value.stream().forEach(s -> sourceList.add(s.getValue())));
			// out.setSource(input.getSource().stream().map(s -> s.getValue()).collect(Collectors.toList()));
			List<Subject> subjectList = new ArrayList<>();
			Optional
				.ofNullable(input.getSubject())
				.ifPresent(
					value -> value
						.forEach(s -> subjectList.add(getSubject(s))));

			out.setSubjects(subjectList);

			out.setType(input.getResulttype().getClassid());
		}

		if (!Constants.DUMPTYPE.COMPLETE.getType().equals(dumpType)) {
			((CommunityResult) out)
				.setCollectedfrom(
					input
						.getCollectedfrom()
						.stream()
						.map(cf -> KeyValue.newInstance(cf.getKey(), cf.getValue()))
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
							String community_id = c.getId();
							if (community_id.indexOf("::") > 0) {
								community_id = community_id.substring(0, community_id.indexOf("::"));
							}
							if (communities.contains(community_id)) {
								Context context = new Context();
								context.setCode(community_id);
								context.setLabel(communityMap.get(community_id));
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
																	provenanceaction.getClassname(), di.getTrust()))
														.orElse(null))
												.filter(Objects::nonNull)
												.collect(Collectors.toSet()));

									context.setProvenance(getUniqueProvenance(provenance));
								}
								return context;
							}
							return null;
						})
						.filter(Objects::nonNull)
						.collect(Collectors.toList()))
				.orElse(new ArrayList<>());

			if (contextList.size() > 0) {
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
		return out;

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
				KeyValue
					.newInstance(i.getCollectedfrom().getKey(), i.getCollectedfrom().getValue()));

		instance
			.setHostedby(
				KeyValue.newInstance(i.getHostedby().getKey(), i.getHostedby().getValue()));

		return instance;

	}

	private static <I extends Instance> void setCommonValue(eu.dnetlib.dhp.schema.oaf.Instance i, I instance) {
		Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> opAr = Optional
			.ofNullable(i.getAccessright());
		if (opAr.isPresent()) {
			if (Constants.accessRightsCoarMap.containsKey(opAr.get().getClassid())) {
				String code = Constants.accessRightsCoarMap.get(opAr.get().getClassid());
				instance
					.setAccessright(
						AccessRight
							.newInstance(
								code,
								Constants.coarCodeLabelMap.get(code),
								Constants.COAR_ACCESS_RIGHT_SCHEMA));
			}
		}

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
			APC apc = new APC();
			apc.setCurrency(oPcc.get().getValue());
			apc.setAmount(oPca.get().getValue());
			instance.setArticleprocessingcharge(apc);
		}

	}

	private static List<Provenance> getUniqueProvenance(List<Provenance> provenance) {
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
		subject.setSubject(ControlledField.newInstance(s.getQualifier().getClassid(), s.getValue()));
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
			Pid pid = getOrcid(oPids.get());
			if (pid != null) {
				a.setPid(pid);
			}
		}

		return a;
	}

	private static Pid getOrcid(List<StructuredProperty> p) {
		for (StructuredProperty pid : p) {
			if (pid.getQualifier().getClassid().equals(ModelConstants.ORCID)) {
				Optional<DataInfo> di = Optional.ofNullable(pid.getDataInfo());
				if (di.isPresent()) {
					return Pid
						.newInstance(
							ControlledField
								.newInstance(
									pid.getQualifier().getClassid(),
									pid.getValue()),
							Provenance
								.newInstance(
									di.get().getProvenanceaction().getClassname(),
									di.get().getTrust()));
				} else {
					return Pid
						.newInstance(
							ControlledField
								.newInstance(
									pid.getQualifier().getClassid(),
									pid.getValue())

						);
				}

			}
		}
		return null;
	}

}
