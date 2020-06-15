
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import javax.swing.text.html.Option;

import org.apache.avro.generic.GenericData;

import eu.dnetlib.dhp.schema.dump.oaf.*;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class Mapper implements Serializable {

	public static <I extends eu.dnetlib.dhp.schema.oaf.Result, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> O map(
		I input, Map<String, String> communityMap) {

		O out = null;
		Optional<eu.dnetlib.dhp.schema.oaf.Qualifier> ort = Optional.ofNullable(input.getResulttype());
		if (ort.isPresent()) {
			switch (ort.get().getClassid()) {
				case "publication":
					out = (O) new Publication();
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
					}
					break;
				case "dataset":
					Dataset d = new Dataset();
					eu.dnetlib.dhp.schema.oaf.Dataset id = (eu.dnetlib.dhp.schema.oaf.Dataset) input;
					Optional.ofNullable(id.getSize()).ifPresent(v -> d.setSize(v.getValue()));
					Optional.ofNullable(id.getVersion()).ifPresent(v -> d.setVersion(v.getValue()));

					d
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

					out = (O) d;

					break;
				case "software":
					Software s = new Software();
					eu.dnetlib.dhp.schema.oaf.Software is = (eu.dnetlib.dhp.schema.oaf.Software) input;
					Optional
						.ofNullable(is.getCodeRepositoryUrl())
						.ifPresent(value -> s.setCodeRepositoryUrl(value.getValue()));
					Optional
						.ofNullable(is.getDocumentationUrl())
						.ifPresent(
							value -> s
								.setDocumentationUrl(
									value
										.stream()
										.map(v -> v.getValue())
										.collect(Collectors.toList())));

					Optional
						.ofNullable(is.getProgrammingLanguage())
						.ifPresent(value -> s.setProgrammingLanguage(value.getClassid()));

					out = (O) s;
					break;
				case "other":
					OtherResearchProduct or = new OtherResearchProduct();
					eu.dnetlib.dhp.schema.oaf.OtherResearchProduct ir = (eu.dnetlib.dhp.schema.oaf.OtherResearchProduct) input;
					or
						.setContactgroup(
							Optional
								.ofNullable(ir.getContactgroup())
								.map(value -> value.stream().map(cg -> cg.getValue()).collect(Collectors.toList()))
								.orElse(null));

					or
						.setContactperson(
							Optional
								.ofNullable(ir.getContactperson())
								.map(value -> value.stream().map(cp -> cp.getValue()).collect(Collectors.toList()))
								.orElse(null));
					or
						.setTool(
							Optional
								.ofNullable(ir.getTool())
								.map(value -> value.stream().map(t -> t.getValue()).collect(Collectors.toList()))
								.orElse(null));
					out = (O) or;
					break;
			}
			Optional<List<eu.dnetlib.dhp.schema.oaf.Author>> oAuthor = Optional.ofNullable(input.getAuthor());
			if (oAuthor.isPresent()) {
				// List<eu.dnetlib.dhp.schema.dump.oaf.Author> authorList = new ArrayList<>();
				out
					.setAuthor(
						oAuthor
							.get()
							.stream()
							.map(oa -> getAuthor(oa))
							.collect(Collectors.toList()));
			}

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

			out
				.setCollectedfrom(
					input
						.getCollectedfrom()
						.stream()
						.map(cf -> KeyValue.newInstance(cf.getKey(), cf.getValue()))
						.collect(Collectors.toList()));

			Set<String> communities = communityMap.keySet();
			List<Context> contextList = input
				.getContext()
				.stream()
				.map(c -> {
					if (communities.contains(c.getId())
						|| communities.contains(c.getId().substring(0, c.getId().indexOf("::")))) {
						Context context = new Context();
						if (!communityMap.containsKey(c.getId())) {
							context.setCode(c.getId().substring(0, c.getId().indexOf("::")));
							context.setLabel(communityMap.get(context.getCode()));
						} else {
							context.setCode(c.getId());
							context.setLabel(communityMap.get(c.getId()));
						}
						Optional<List<DataInfo>> dataInfo = Optional.ofNullable(c.getDataInfo());
						if (dataInfo.isPresent()) {
							List<String> provenance = new ArrayList<>();
							provenance
								.addAll(
									dataInfo
										.get()
										.stream()
										.map(di -> {
											if (di.getInferred()) {
												return di.getProvenanceaction().getClassname();
											}
											return null;
										})
										.filter(Objects::nonNull)
										.collect(Collectors.toSet()));
							context.setProvenance(provenance);
						}
						return context;
					}
					return null;
				})
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
			if (contextList.size() > 0) {
				out.setContext(contextList);
			}
			final List<String> contributorList = new ArrayList<>();
			Optional
				.ofNullable(input.getContributor())
				.ifPresent(value -> value.stream().forEach(c -> contributorList.add(c.getValue())));
			out.setContributor(contributorList);

			List<Country> countryList = new ArrayList<>();
			Optional
				.ofNullable(input.getCountry())
				.ifPresent(
					value -> value
						.stream()
						.forEach(
							c -> {
								Country country = new Country();
								country.setCode(c.getClassid());
								country.setLabel(c.getClassname());
								Optional
									.ofNullable(c.getDataInfo())
									.ifPresent(
										provenance -> country
											.setProvenance(
												provenance
													.getProvenanceaction()
													.getClassname()));
								countryList
									.add(country);
							}));

			out.setCountry(countryList);

			final List<String> coverageList = new ArrayList<>();
			Optional
				.ofNullable(input.getCoverage())
				.ifPresent(value -> value.stream().forEach(c -> coverageList.add(c.getValue())));
			out.setCoverage(coverageList);

			out.setDateofcollection(input.getDateofcollection());

			final List<String> descriptionList = new ArrayList<>();
			Optional
				.ofNullable(input.getDescription())
				.ifPresent(value -> value.stream().forEach(d -> descriptionList.add(d.getValue())));
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

			final List<Instance> instanceList = new ArrayList<>();
			Optional
				.ofNullable(input.getInstance())
				.ifPresent(
					inst -> inst
						.stream()
						.forEach(i -> {
							Instance instance = new Instance();

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

							instance
								.setCollectedfrom(
									KeyValue
										.newInstance(i.getCollectedfrom().getKey(), i.getCollectedfrom().getValue()));
							instance
								.setHostedby(
									KeyValue.newInstance(i.getHostedby().getKey(), i.getHostedby().getValue()));
							Optional
								.ofNullable(i.getLicense())
								.ifPresent(value -> instance.setLicense(value.getValue()));
							Optional
								.ofNullable(i.getDateofacceptance())
								.ifPresent(value -> instance.setPublicationdate(value.getValue()));
							Optional
								.ofNullable(i.getRefereed())
								.ifPresent(value -> instance.setRefereed(value.getValue()));
							Optional
								.ofNullable(i.getInstancetype())
								.ifPresent(value -> instance.setType(value.getClassname()));
							Optional.ofNullable(i.getUrl()).ifPresent(value -> instance.setUrl(value));
							instanceList.add(instance);
						}));
			out
				.setInstance(instanceList);

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
			List<ControlledField> subjectList = new ArrayList<>();
			Optional
				.ofNullable(input.getSubject())
				.ifPresent(
					value -> value
						.stream()
						.forEach(
							s -> subjectList
								.add(ControlledField.newInstance(s.getQualifier().getClassid(), s.getValue()))));
			out.setSubject(subjectList);

			out.setType(input.getResulttype().getClassid());
		}

		return out;
	}

	private static Author getAuthor(eu.dnetlib.dhp.schema.oaf.Author oa) {
		Author a = new Author();
		Optional
			.ofNullable(oa.getAffiliation())
			.ifPresent(
				value -> a
					.setAffiliation(
						value
							.stream()
							.map(aff -> aff.getValue())
							.collect(Collectors.toList())));
		a.setFullname(oa.getFullname());
		a.setName(oa.getName());
		a.setSurname(oa.getSurname());
		a.setRank(oa.getRank());
		Optional
			.ofNullable(oa.getPid())
			.ifPresent(
				value -> a
					.setPid(
						value
							.stream()
							.map(p -> ControlledField.newInstance(p.getQualifier().getClassid(), p.getValue()))
							.collect(Collectors.toList())));
		return a;
	}

}
