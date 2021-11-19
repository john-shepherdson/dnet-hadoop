
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static com.jayway.jsonpath.Filter.filter;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.*;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Project;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.OafEntity;

/**
 * Dumps of entities in the model defined in eu.dnetlib.dhp.schema.dump.oaf.graph. Results are dumped using the same
 * Mapper as for eu.dnetlib.dhp.schema.dump.oaf.community, while for the other entities the mapping is defined below
 */
public class DumpGraphEntities implements Serializable {

	public void run(Boolean isSparkSessionManaged,
		String inputPath,
		String outputPath,
		Class<? extends OafEntity> inputClazz,
		String communityMapPath) {

		SparkConf conf = new SparkConf();

		switch (ModelSupport.idPrefixMap.get(inputClazz)) {
			case "50":
				DumpProducts d = new DumpProducts();
				d
					.run(
						isSparkSessionManaged, inputPath, outputPath, communityMapPath, inputClazz, GraphResult.class,
						eu.dnetlib.dhp.oa.graph.dump.Constants.DUMPTYPE.COMPLETE.getType());
				break;
			case "40":
				runWithSparkSession(
					conf,
					isSparkSessionManaged,
					spark -> {
						Utils.removeOutputDir(spark, outputPath);
						projectMap(spark, inputPath, outputPath, inputClazz);

					});
				break;
			case "20":
				runWithSparkSession(
					conf,
					isSparkSessionManaged,
					spark -> {
						Utils.removeOutputDir(spark, outputPath);
						organizationMap(spark, inputPath, outputPath, inputClazz);

					});
				break;
			case "10":
				runWithSparkSession(
					conf,
					isSparkSessionManaged,
					spark -> {
						Utils.removeOutputDir(spark, outputPath);
						datasourceMap(spark, inputPath, outputPath, inputClazz);

					});
				break;
		}

	}

	private static <E extends OafEntity> void datasourceMap(SparkSession spark, String inputPath, String outputPath,
		Class<E> inputClazz) {
		Utils
			.readPath(spark, inputPath, inputClazz)
			.map(
				(MapFunction<E, Datasource>) d -> mapDatasource((eu.dnetlib.dhp.schema.oaf.Datasource) d),
				Encoders.bean(Datasource.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <E extends OafEntity> void projectMap(SparkSession spark, String inputPath, String outputPath,
		Class<E> inputClazz) {
		Utils
			.readPath(spark, inputPath, inputClazz)
			.map(
				(MapFunction<E, Project>) p -> mapProject((eu.dnetlib.dhp.schema.oaf.Project) p),
				Encoders.bean(Project.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static Datasource mapDatasource(eu.dnetlib.dhp.schema.oaf.Datasource d) {
		Datasource datasource = new Datasource();

		datasource.setId(d.getId());

		Optional
			.ofNullable(d.getOriginalId())
			.ifPresent(
				oId -> datasource.setOriginalId(oId.stream().filter(Objects::nonNull).collect(Collectors.toList())));

		Optional
			.ofNullable(d.getPid())
			.ifPresent(
				pids -> pids
					.stream()
					.map(p -> ControlledField.newInstance(p.getQualifier().getClassid(), p.getValue()))
					.collect(Collectors.toList()));

		Optional
			.ofNullable(d.getDatasourcetype())
			.ifPresent(
				dsType -> datasource
					.setDatasourcetype(DatasourceSchemeValue.newInstance(dsType.getClassid(), dsType.getClassname())));

		Optional
			.ofNullable(d.getOpenairecompatibility())
			.ifPresent(v -> datasource.setOpenairecompatibility(v.getClassname()));

		Optional
			.ofNullable(d.getOfficialname())
			.ifPresent(oname -> datasource.setOfficialname(oname.getValue()));

		Optional
			.ofNullable(d.getEnglishname())
			.ifPresent(ename -> datasource.setEnglishname(ename.getValue()));

		Optional
			.ofNullable(d.getWebsiteurl())
			.ifPresent(wsite -> datasource.setWebsiteurl(wsite.getValue()));

		Optional
			.ofNullable(d.getLogourl())
			.ifPresent(lurl -> datasource.setLogourl(lurl.getValue()));

		Optional
			.ofNullable(d.getDateofvalidation())
			.ifPresent(dval -> datasource.setDateofvalidation(dval.getValue()));

		Optional
			.ofNullable(d.getDescription())
			.ifPresent(dex -> datasource.setDescription(dex.getValue()));

		Optional
			.ofNullable(d.getSubjects())
			.ifPresent(
				sbjs -> datasource.setSubjects(sbjs.stream().map(sbj -> sbj.getValue()).collect(Collectors.toList())));

		Optional
			.ofNullable(d.getOdpolicies())
			.ifPresent(odp -> datasource.setPolicies(Arrays.asList(odp.getValue())));

		Optional
			.ofNullable(d.getOdlanguages())
			.ifPresent(
				langs -> datasource
					.setLanguages(langs.stream().map(lang -> lang.getValue()).collect(Collectors.toList())));

		Optional
			.ofNullable(d.getOdcontenttypes())
			.ifPresent(
				ctypes -> datasource
					.setContenttypes(ctypes.stream().map(ctype -> ctype.getValue()).collect(Collectors.toList())));

		Optional
			.ofNullable(d.getReleasestartdate())
			.ifPresent(rd -> datasource.setReleasestartdate(rd.getValue()));

		Optional
			.ofNullable(d.getReleaseenddate())
			.ifPresent(ed -> datasource.setReleaseenddate(ed.getValue()));

		Optional
			.ofNullable(d.getMissionstatementurl())
			.ifPresent(ms -> datasource.setMissionstatementurl(ms.getValue()));

		Optional
			.ofNullable(d.getDatabaseaccesstype())
			.ifPresent(ar -> datasource.setAccessrights(ar.getValue()));

		Optional
			.ofNullable(d.getDatauploadtype())
			.ifPresent(dut -> datasource.setUploadrights(dut.getValue()));

		Optional
			.ofNullable(d.getDatabaseaccessrestriction())
			.ifPresent(dar -> datasource.setDatabaseaccessrestriction(dar.getValue()));

		Optional
			.ofNullable(d.getDatauploadrestriction())
			.ifPresent(dur -> datasource.setDatauploadrestriction(dur.getValue()));

		Optional
			.ofNullable(d.getVersioning())
			.ifPresent(v -> datasource.setVersioning(v.getValue()));

		Optional
			.ofNullable(d.getCitationguidelineurl())
			.ifPresent(cu -> datasource.setCitationguidelineurl(cu.getValue()));

		Optional
			.ofNullable(d.getPidsystems())
			.ifPresent(ps -> datasource.setPidsystems(ps.getValue()));

		Optional
			.ofNullable(d.getCertificates())
			.ifPresent(c -> datasource.setCertificates(c.getValue()));

		Optional
			.ofNullable(d.getPolicies())
			.ifPresent(ps -> datasource.setPolicies(ps.stream().map(p -> p.getValue()).collect(Collectors.toList())));

		Optional
			.ofNullable(d.getJournal())
			.ifPresent(j -> datasource.setJournal(getContainer(j)));

		return datasource;

	}

	private static Container getContainer(Journal j) {
		Container c = new Container();

		Optional
			.ofNullable(j.getName())
			.ifPresent(n -> c.setName(n));

		Optional
			.ofNullable(j.getIssnPrinted())
			.ifPresent(issnp -> c.setIssnPrinted(issnp));

		Optional
			.ofNullable(j.getIssnOnline())
			.ifPresent(issno -> c.setIssnOnline(issno));

		Optional
			.ofNullable(j.getIssnLinking())
			.ifPresent(isnl -> c.setIssnLinking(isnl));

		Optional
			.ofNullable(j.getEp())
			.ifPresent(ep -> c.setEp(ep));

		Optional
			.ofNullable(j.getIss())
			.ifPresent(iss -> c.setIss(iss));

		Optional
			.ofNullable(j.getSp())
			.ifPresent(sp -> c.setSp(sp));

		Optional
			.ofNullable(j.getVol())
			.ifPresent(vol -> c.setVol(vol));

		Optional
			.ofNullable(j.getEdition())
			.ifPresent(edition -> c.setEdition(edition));

		Optional
			.ofNullable(j.getConferencedate())
			.ifPresent(cdate -> c.setConferencedate(cdate));

		Optional
			.ofNullable(j.getConferenceplace())
			.ifPresent(cplace -> c.setConferenceplace(cplace));

		return c;
	}

	private static Project mapProject(eu.dnetlib.dhp.schema.oaf.Project p) throws DocumentException {
		Project project = new Project();

		Optional
			.ofNullable(p.getId())
			.ifPresent(id -> project.setId(id));

		Optional
			.ofNullable(p.getWebsiteurl())
			.ifPresent(w -> project.setWebsiteurl(w.getValue()));

		Optional
			.ofNullable(p.getCode())
			.ifPresent(code -> project.setCode(code.getValue()));

		Optional
			.ofNullable(p.getAcronym())
			.ifPresent(acronynim -> project.setAcronym(acronynim.getValue()));

		Optional
			.ofNullable(p.getTitle())
			.ifPresent(title -> project.setTitle(title.getValue()));

		Optional
			.ofNullable(p.getStartdate())
			.ifPresent(sdate -> project.setStartdate(sdate.getValue()));

		Optional
			.ofNullable(p.getEnddate())
			.ifPresent(edate -> project.setEnddate(edate.getValue()));

		Optional
			.ofNullable(p.getCallidentifier())
			.ifPresent(cide -> project.setCallidentifier(cide.getValue()));

		Optional
			.ofNullable(p.getKeywords())
			.ifPresent(key -> project.setKeywords(key.getValue()));

		Optional<Field<String>> omandate = Optional.ofNullable(p.getOamandatepublications());
		Optional<Field<String>> oecsc39 = Optional.ofNullable(p.getEcsc39());
		boolean mandate = false;
		if (omandate.isPresent()) {
			if (omandate.get().getValue().equals("true")) {
				mandate = true;
			}
		}
		if (oecsc39.isPresent()) {
			if (oecsc39.get().getValue().equals("true")) {
				mandate = true;
			}
		}

		project.setOpenaccessmandateforpublications(mandate);
		project.setOpenaccessmandatefordataset(false);

		Optional
			.ofNullable(p.getEcarticle29_3())
			.ifPresent(oamandate -> project.setOpenaccessmandatefordataset(oamandate.getValue().equals("true")));

		project
			.setSubject(
				Optional
					.ofNullable(p.getSubjects())
					.map(subjs -> subjs.stream().map(s -> s.getValue()).collect(Collectors.toList()))
					.orElse(new ArrayList<>()));

		Optional
			.ofNullable(p.getSummary())
			.ifPresent(summary -> project.setSummary(summary.getValue()));

		Optional<Float> ofundedamount = Optional.ofNullable(p.getFundedamount());
		Optional<Field<String>> ocurrency = Optional.ofNullable(p.getCurrency());
		Optional<Float> ototalcost = Optional.ofNullable(p.getTotalcost());

		if (ocurrency.isPresent()) {
			if (ofundedamount.isPresent()) {
				if (ototalcost.isPresent()) {
					project
						.setGranted(
							Granted.newInstance(ocurrency.get().getValue(), ototalcost.get(), ofundedamount.get()));
				} else {
					project.setGranted(Granted.newInstance(ocurrency.get().getValue(), ofundedamount.get()));
				}
			}
		}

		project
			.setH2020programme(
				Optional
					.ofNullable(p.getH2020classification())
					.map(
						classification -> classification
							.stream()
							.map(
								c -> Programme
									.newInstance(
										c.getH2020Programme().getCode(), c.getH2020Programme().getDescription()))
							.collect(Collectors.toList()))
					.orElse(new ArrayList<>()));

		Optional<List<Field<String>>> ofundTree = Optional
			.ofNullable(p.getFundingtree());
		List<Funder> funList = new ArrayList<>();
		if (ofundTree.isPresent()) {
			for (Field<String> fundingtree : ofundTree.get()) {
				funList.add(getFunder(fundingtree.getValue()));
			}
		}
		project.setFunding(funList);

		return project;
	}

	public static Funder getFunder(String fundingtree) throws DocumentException {
		Funder f = new Funder();
		final Document doc;

		doc = new SAXReader().read(new StringReader(fundingtree));
		f.setShortName(((org.dom4j.Node) (doc.selectNodes("//funder/shortname").get(0))).getText());
		f.setName(((org.dom4j.Node) (doc.selectNodes("//funder/name").get(0))).getText());
		f.setJurisdiction(((org.dom4j.Node) (doc.selectNodes("//funder/jurisdiction").get(0))).getText());
		// f.setId(((org.dom4j.Node) (doc.selectNodes("//funder/id").get(0))).getText());

		String id = "";
		String description = "";
		// List<Levels> fundings = new ArrayList<>();
		int level = 0;
		List<org.dom4j.Node> nodes = doc.selectNodes("//funding_level_" + level);
		while (nodes.size() > 0) {
			for (org.dom4j.Node n : nodes) {

				List node = n.selectNodes("./id");
				id = ((org.dom4j.Node) node.get(0)).getText();
				id = id.substring(id.indexOf("::") + 2);

				node = n.selectNodes("./description");
				description += ((Node) node.get(0)).getText() + " - ";

			}
			level += 1;
			nodes = doc.selectNodes("//funding_level_" + level);
		}

		if (!id.equals("")) {
			Fundings fundings = new Fundings();
			fundings.setId(id);
			fundings.setDescription(description.substring(0, description.length() - 3).trim());
			f.setFunding_stream(fundings);
		}

		return f;

	}

	private static <E extends OafEntity> void organizationMap(SparkSession spark, String inputPath, String outputPath,
		Class<E> inputClazz) {
		Utils
			.readPath(spark, inputPath, inputClazz)
			.map(
				(MapFunction<E, Organization>) o -> mapOrganization((eu.dnetlib.dhp.schema.oaf.Organization) o),
				Encoders.bean(Organization.class))
			.filter((FilterFunction<Organization>) o -> o != null)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static eu.dnetlib.dhp.schema.dump.oaf.graph.Organization mapOrganization(
		eu.dnetlib.dhp.schema.oaf.Organization org) {
		if (org.getDataInfo().getDeletedbyinference())
			return null;
		Organization organization = new Organization();

		Optional
			.ofNullable(org.getLegalshortname())
			.ifPresent(value -> organization.setLegalshortname(value.getValue()));

		Optional
			.ofNullable(org.getLegalname())
			.ifPresent(value -> organization.setLegalname(value.getValue()));

		Optional
			.ofNullable(org.getWebsiteurl())
			.ifPresent(value -> organization.setWebsiteurl(value.getValue()));

		Optional
			.ofNullable(org.getAlternativeNames())
			.ifPresent(
				value -> organization
					.setAlternativenames(
						value
							.stream()
							.map(v -> v.getValue())
							.collect(Collectors.toList())));

		Optional
			.ofNullable(org.getCountry())
			.ifPresent(
				value -> {
					if (!value.getClassid().equals(Constants.UNKNOWN)) {
						organization.setCountry(Country.newInstance(value.getClassid(), value.getClassname()));
					}

				});

		Optional
			.ofNullable(org.getId())
			.ifPresent(value -> organization.setId(value));

		Optional
			.ofNullable(org.getPid())
			.ifPresent(
				value -> organization
					.setPid(
						value
							.stream()
							.map(p -> OrganizationPid.newInstance(p.getQualifier().getClassid(), p.getValue()))
							.collect(Collectors.toList())));

		return organization;
	}
}
