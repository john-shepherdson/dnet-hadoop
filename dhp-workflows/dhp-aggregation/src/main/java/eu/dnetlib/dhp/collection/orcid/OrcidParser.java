
package eu.dnetlib.dhp.collection.orcid;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximpleware.*;

import eu.dnetlib.dhp.collection.orcid.model.*;
import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;

public class OrcidParser {

	final Logger log = LoggerFactory.getLogger(OrcidParser.class);
	private VTDNav vn;

	private AutoPilot ap;
	private static final String NS_COMMON_URL = "http://www.orcid.org/ns/common";
	private static final String NS_COMMON = "common";
	private static final String NS_PERSON_URL = "http://www.orcid.org/ns/person";
	private static final String NS_PERSON = "person";
	private static final String NS_DETAILS_URL = "http://www.orcid.org/ns/personal-details";
	private static final String NS_DETAILS = "personal-details";
	private static final String NS_OTHER_URL = "http://www.orcid.org/ns/other-name";
	private static final String NS_OTHER = "other-name";
	private static final String NS_RECORD_URL = "http://www.orcid.org/ns/record";
	private static final String NS_RECORD = "record";
	private static final String NS_ERROR_URL = "http://www.orcid.org/ns/error";
	private static final String NS_ACTIVITIES = "activities";
	private static final String NS_ACTIVITIES_URL = "http://www.orcid.org/ns/activities";
	private static final String NS_WORK = "work";
	private static final String NS_WORK_URL = "http://www.orcid.org/ns/work";

	private static final String NS_ERROR = "error";
	private static final String NS_HISTORY = "history";
	private static final String NS_HISTORY_URL = "http://www.orcid.org/ns/history";
	private static final String NS_EMPLOYMENT = "employment";
	private static final String NS_EMPLOYMENT_URL = "http://www.orcid.org/ns/employment";
	private static final String NS_EXTERNAL = "external-identifier";
	private static final String NS_EXTERNAL_URL = "http://www.orcid.org/ns/external-identifier";

	private void generateParsedDocument(final String xml) throws ParseException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(xml.getBytes());
		vg.parse(true);
		this.vn = vg.getNav();
		this.ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_PERSON, NS_PERSON_URL);
		ap.declareXPathNameSpace(NS_DETAILS, NS_DETAILS_URL);
		ap.declareXPathNameSpace(NS_OTHER, NS_OTHER_URL);
		ap.declareXPathNameSpace(NS_RECORD, NS_RECORD_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);
		ap.declareXPathNameSpace(NS_HISTORY, NS_HISTORY_URL);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		ap.declareXPathNameSpace(NS_EXTERNAL, NS_EXTERNAL_URL);
		ap.declareXPathNameSpace(NS_ACTIVITIES, NS_ACTIVITIES_URL);
		ap.declareXPathNameSpace(NS_EMPLOYMENT, NS_EMPLOYMENT_URL);
	}

	public Author parseSummary(final String xml) {

		try {
			final Author author = new Author();
			generateParsedDocument(xml);
			List<VtdUtilityParser.Node> recordNodes = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap, vn, "//record:record", Collections.singletonList("path"));
			if (!recordNodes.isEmpty()) {
				final String oid = (recordNodes.get(0).getAttributes().get("path")).substring(1);
				author.setOrcid(oid);
			} else {
				return null;
			}
			final String ltm = VtdUtilityParser.getSingleValue(ap, vn, "//common:last-modified-date");
			author.setLastModifiedDate(ltm);
			List<VtdUtilityParser.Node> personNodes = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap, vn, "//person:name", Arrays.asList("visibility"));
			final String visibility = (personNodes.get(0).getAttributes().get("visibility"));
			author.setVisibility(visibility);
			final String name = VtdUtilityParser.getSingleValue(ap, vn, "//personal-details:given-names");
			author.setGivenName(name);

			final String surnames = VtdUtilityParser.getSingleValue(ap, vn, "//personal-details:family-name");
			author.setFamilyName(surnames);

			final String creditNames = VtdUtilityParser.getSingleValue(ap, vn, "//personal-details:credit-name");
			author.setCreditName(creditNames);

			final String biography = VtdUtilityParser
				.getSingleValue(ap, vn, "//person:biography/personal-details:content");
			author.setBiography(biography);

			final List<String> otherNames = VtdUtilityParser.getTextValue(ap, vn, "//other-name:content");
			if (!otherNames.isEmpty()) {
				author.setOtherNames(otherNames);
			}

			ap.selectXPath("//external-identifier:external-identifier");

			while (ap.evalXPath() != -1) {
				final Pid pid = new Pid();

				final AutoPilot ap1 = new AutoPilot(ap.getNav());

				ap1.selectXPath("./common:external-id-type");
				while (ap1.evalXPath() != -1) {
					int it = vn.getText();
					pid.setSchema(vn.toNormalizedString(it));
				}
				ap1.selectXPath("./common:external-id-value");
				while (ap1.evalXPath() != -1) {
					int it = vn.getText();
					pid.setValue(vn.toNormalizedString(it));
				}

				author.addOtherPid(pid);
			}

			return author;
		} catch (Throwable e) {
			log.error("Error on parsing {}", xml);
			log.error(e.getMessage());
			return null;
		}
	}

	public List<Work> parseWorks(final String xml) {

		try {
			String oid;

			generateParsedDocument(xml);
			List<VtdUtilityParser.Node> workNodes = VtdUtilityParser
				.getTextValuesWithAttributes(ap, vn, "//activities:works", Arrays.asList("path", "visibility"));
			if (!workNodes.isEmpty()) {
				oid = (workNodes.get(0).getAttributes().get("path")).split("/")[1];

			} else {
				return null;
			}
			final List<Work> works = new ArrayList<>();
			ap.selectXPath("//work:work-summary");

			while (ap.evalXPath() != -1) {
				final Work work = new Work();
				work.setOrcid(oid);
				final AutoPilot ap1 = new AutoPilot(ap.getNav());
				ap1.selectXPath("./work:title/common:title");
				while (ap1.evalXPath() != -1) {
					int it = vn.getText();
					work.setTitle(vn.toNormalizedString(it));
				}
				ap1.selectXPath(".//common:external-id");
				while (ap1.evalXPath() != -1) {
					final Pid pid = new Pid();

					final AutoPilot ap2 = new AutoPilot(ap1.getNav());

					ap2.selectXPath("./common:external-id-type");
					while (ap2.evalXPath() != -1) {
						int it = vn.getText();
						pid.setSchema(vn.toNormalizedString(it));
					}
					ap2.selectXPath("./common:external-id-value");
					while (ap2.evalXPath() != -1) {
						int it = vn.getText();
						pid.setValue(vn.toNormalizedString(it));
					}

					work.addPid(pid);
				}

				works.add(work);
			}
			return works;

		} catch (Throwable e) {
			log.error("Error on parsing {}", xml);
			log.error(e.getMessage());
			return null;
		}

	}

	public Work parseWork(final String xml) {

		try {
			final Work work = new Work();
			generateParsedDocument(xml);
			List<VtdUtilityParser.Node> workNodes = VtdUtilityParser
				.getTextValuesWithAttributes(ap, vn, "//work:work", Arrays.asList("path", "visibility"));
			if (!workNodes.isEmpty()) {
				final String oid = (workNodes.get(0).getAttributes().get("path")).split("/")[1];
				work.setOrcid(oid);
			} else {
				return null;
			}

			ap.selectXPath("//common:external-id");

			while (ap.evalXPath() != -1) {
				final Pid pid = new Pid();

				final AutoPilot ap1 = new AutoPilot(ap.getNav());

				ap1.selectXPath("./common:external-id-type");
				while (ap1.evalXPath() != -1) {
					int it = vn.getText();
					pid.setSchema(vn.toNormalizedString(it));
				}
				ap1.selectXPath("./common:external-id-value");
				while (ap1.evalXPath() != -1) {
					int it = vn.getText();
					pid.setValue(vn.toNormalizedString(it));
				}

				work.addPid(pid);
			}

			work.setTitle(VtdUtilityParser.getSingleValue(ap, vn, "//work:title/common:title"));

			return work;
		} catch (Throwable e) {
			log.error("Error on parsing {}", xml);
			log.error(e.getMessage());
			return null;
		}

	}

	private String extractEmploymentDate(final String xpath) throws Exception {
		return extractEmploymentDate(xpath, ap);
	}

	private String extractEmploymentDate(final String xpath, AutoPilot pp) throws Exception {

		pp.selectXPath(xpath);
		StringBuilder sb = new StringBuilder();
		while (pp.evalXPath() != -1) {
			final AutoPilot ap1 = new AutoPilot(pp.getNav());
			ap1.selectXPath("./common:year");
			while (ap1.evalXPath() != -1) {
				int it = vn.getText();
				sb.append(vn.toNormalizedString(it));
			}
			ap1.selectXPath("./common:month");
			while (ap1.evalXPath() != -1) {
				int it = vn.getText();
				sb.append("-");
				sb.append(vn.toNormalizedString(it));
			}
			ap1.selectXPath("./common:day");
			while (ap1.evalXPath() != -1) {
				int it = vn.getText();
				sb.append("-");
				sb.append(vn.toNormalizedString(it));
			}
		}
		return sb.toString();

	}

	public List<Employment> parseEmployments(final String xml) {
		try {
			String oid;
			Map<String, String> nsContext = getNameSpaceMap();
			DocumentFactory.getInstance().setXPathNamespaceURIs(nsContext);
			Document doc = DocumentHelper.parseText(xml);
			oid = doc.valueOf("//activities:employments/@path");
			if (oid == null || StringUtils.isEmpty(oid))
				return null;
			final String orcid = oid.split("/")[1];

			List<Node> nodes = doc.selectNodes("//employment:employment-summary");
			return nodes.stream().map(n -> {
				final Employment e = new Employment();
				e.setOrcid(orcid);

				final String depName = n.valueOf(".//common:department-name");
				if (StringUtils.isNotBlank(depName))
					e.setDepartmentName(depName);
				final String roleTitle = n.valueOf(".//common:role-title");
				e.setRoleTitle(roleTitle);
				final String organizationName = n.valueOf(".//common:organization/common:name");
				if (StringUtils.isEmpty(e.getDepartmentName()))
					e.setDepartmentName(organizationName);
				final Pid p = new Pid();
				final String pid = n
					.valueOf(
						"./common:organization/common:disambiguated-organization/common:disambiguated-organization-identifier");
				p.setValue(pid);
				final String pidType = n
					.valueOf("./common:organization/common:disambiguated-organization/common:disambiguation-source");
				p.setSchema(pidType);
				e.setAffiliationId(p);

				final StringBuilder aDate = new StringBuilder();
				final String sy = n.valueOf("./common:start-date/common:year");
				if (StringUtils.isNotBlank(sy)) {
					aDate.append(sy);
					final String sm = n.valueOf("./common:start-date/common:month");
					final String sd = n.valueOf("./common:start-date/common:day");
					aDate.append("-");
					if (StringUtils.isNotBlank(sm))
						aDate.append(sm);
					else
						aDate.append("01");
					aDate.append("-");
					if (StringUtils.isNotBlank(sd))
						aDate.append(sd);
					else
						aDate.append("01");
					e.setEndDate(aDate.toString());
				}

				final String ey = n.valueOf("./common:end-date/common:year");
				if (StringUtils.isNotBlank(ey)) {
					aDate.append(ey);
					final String em = n.valueOf("./common:end-date/common:month");
					final String ed = n.valueOf("./common:end-date/common:day");
					aDate.append("-");
					if (StringUtils.isNotBlank(em))
						aDate.append(em);
					else
						aDate.append("01");
					aDate.append("-");
					if (StringUtils.isNotBlank(ed))
						aDate.append(ed);
					else
						aDate.append("01");
					e.setEndDate(aDate.toString());
				}

				return e;

			}).collect(Collectors.toList());
		} catch (Throwable e) {
			log.error("Error on parsing {}", xml);
			log.error(e.getMessage());
			return null;
		}
	}

	@NotNull
	private static Map<String, String> getNameSpaceMap() {
		Map<String, String> nsContext = new HashMap<>();
		nsContext.put(NS_COMMON, NS_COMMON_URL);
		nsContext.put(NS_PERSON, NS_PERSON_URL);
		nsContext.put(NS_DETAILS, NS_DETAILS_URL);
		nsContext.put(NS_OTHER, NS_OTHER_URL);
		nsContext.put(NS_RECORD, NS_RECORD_URL);
		nsContext.put(NS_ERROR, NS_ERROR_URL);
		nsContext.put(NS_HISTORY, NS_HISTORY_URL);
		nsContext.put(NS_WORK, NS_WORK_URL);
		nsContext.put(NS_EXTERNAL, NS_EXTERNAL_URL);
		nsContext.put(NS_ACTIVITIES, NS_ACTIVITIES_URL);
		nsContext.put(NS_EMPLOYMENT, NS_EMPLOYMENT_URL);
		return nsContext;
	}

	public Employment parseEmployment(final String xml) {
		try {
			final Employment employment = new Employment();
			generateParsedDocument(xml);
			final String oid = VtdUtilityParser
				.getSingleValue(ap, vn, "//common:source-orcid/common:path");
			if (StringUtils.isNotBlank(oid)) {
				employment.setOrcid(oid);
			} else {
				return null;
			}
			final String depName = VtdUtilityParser
				.getSingleValue(ap, vn, "//common:department-name");
			final String rolTitle = VtdUtilityParser
				.getSingleValue(ap, vn, "//common:role-title");
			if (StringUtils.isNotBlank(rolTitle))
				employment.setRoleTitle(rolTitle);
			if (StringUtils.isNotBlank(depName))
				employment.setDepartmentName(depName);
			else
				employment
					.setDepartmentName(
						VtdUtilityParser
							.getSingleValue(ap, vn, "//common:organization/common:name"));

			employment.setStartDate(extractEmploymentDate("//common:start-date"));
			employment.setEndDate(extractEmploymentDate("//common:end-date"));

			final String affiliationId = VtdUtilityParser
				.getSingleValue(ap, vn, "//common:disambiguated-organization-identifier");
			final String affiliationIdType = VtdUtilityParser
				.getSingleValue(ap, vn, "//common:disambiguation-source");

			if (StringUtils.isNotBlank(affiliationId) || StringUtils.isNotBlank(affiliationIdType))
				employment.setAffiliationId(new Pid(affiliationId, affiliationIdType));

			return employment;
		} catch (Throwable e) {
			log.error("Error on parsing {}", xml);
			log.error(e.getMessage());
			return null;
		}

	}

}
