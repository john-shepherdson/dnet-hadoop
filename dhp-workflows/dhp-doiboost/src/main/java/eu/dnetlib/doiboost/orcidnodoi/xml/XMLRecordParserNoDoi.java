
package eu.dnetlib.doiboost.orcidnodoi.xml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximpleware.*;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.doiboost.orcidnodoi.model.Contributor;
import eu.dnetlib.doiboost.orcidnodoi.model.ExternalId;
import eu.dnetlib.doiboost.orcidnodoi.model.PublicationDate;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;

public class XMLRecordParserNoDoi {

	private static final Logger logger = LoggerFactory.getLogger(XMLRecordParserNoDoi.class);

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

	private static final String NS_WORK = "work";
	private static final String NS_WORK_URL = "http://www.orcid.org/ns/work";

	private static final String NS_ERROR = "error";

	public static WorkDataNoDoi VTDParseWorkData(byte[] bytes)
		throws VtdException, EncodingException, EOFException, EntityException, ParseException, XPathParseException,
		NavException, XPathEvalException {
		logger.info("parsing xml ...");
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);

		WorkDataNoDoi workData = new WorkDataNoDoi();
		final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
		if (!errors.isEmpty()) {
			workData.setErrorCode(errors.get(0));
			return workData;
		}

		List<VtdUtilityParser.Node> workNodes = VtdUtilityParser
			.getTextValuesWithAttributes(ap, vn, "//work:work", Arrays.asList("path", "put-code"));
		if (!workNodes.isEmpty()) {
			final String oid = (workNodes.get(0).getAttributes().get("path")).split("/")[1];
			workData.setOid(oid);
			final String id = (workNodes.get(0).getAttributes().get("put-code"));
			workData.setId(id);
		} else {
			return null;
		}

		final List<String> titles = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:title");
		if (!titles.isEmpty()) {
			workData.setTitles(titles);
		}

		final List<String> sourceNames = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:source-name");
		if (!sourceNames.isEmpty()) {
			workData.setSourceName(sourceNames.get(0));
		}

		final List<String> types = VtdUtilityParser
			.getTextValue(
				ap, vn, "//work:type");
		if (!types.isEmpty()) {
			workData.setType(types.get(0));
		}

		final List<String> urls = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:url");
		if (!urls.isEmpty()) {
			workData.setUrls(urls);
		}

		workData.setPublicationDates(getPublicationDates(vg, vn, ap));
		workData.setExtIds(getExternalIds(vg, vn, ap));
		workData.setContributors(getContributors(vg, vn, ap));
		return workData;

	}

	private static List<PublicationDate> getPublicationDates(VTDGen vg, VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<PublicationDate> publicationDates = new ArrayList<PublicationDate>();
		int yearIndex = 0;
		ap.selectXPath("//common:publication-date/common:year");
		while (ap.evalXPath() != -1) {
			PublicationDate publicationDate = new PublicationDate();
			int t = vn.getText();
			if (t >= 0) {
				publicationDate.setYear(vn.toNormalizedString(t));
				publicationDates.add(yearIndex, publicationDate);
				yearIndex++;
			}
		}
		int monthIndex = 0;
		ap.selectXPath("//common:publication-date/common:month");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				publicationDates.get(monthIndex).setMonth(vn.toNormalizedString(t));
				monthIndex++;
			}
		}
		int dayIndex = 0;
		ap.selectXPath("//common:publication-date/common:day");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				publicationDates.get(dayIndex).setDay(vn.toNormalizedString(t));
				dayIndex++;
			}
		}
		return publicationDates;
	}

	private static List<ExternalId> getExternalIds(VTDGen vg, VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<ExternalId> extIds = new ArrayList<ExternalId>();
		int typeIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-type");
		while (ap.evalXPath() != -1) {
			ExternalId extId = new ExternalId();
			int t = vn.getText();
			if (t >= 0) {
				extId.setType(vn.toNormalizedString(t));
				extIds.add(typeIndex, extId);
				typeIndex++;
			}
		}
		int valueIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-value");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				extIds.get(valueIndex).setValue(vn.toNormalizedString(t));
				valueIndex++;
			}
		}
		int relationshipIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-relationship");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				extIds.get(relationshipIndex).setRelationShip(vn.toNormalizedString(t));
				relationshipIndex++;
			}
		}
		if (typeIndex == valueIndex) {
			return extIds;
		}
		return new ArrayList<ExternalId>();
	}

	private static List<Contributor> getContributors(VTDGen vg, VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<Contributor> contributors = new ArrayList<Contributor>();
		int nameIndex = 0;
		ap.selectXPath("//work:contributor/work:credit-name");
		while (ap.evalXPath() != -1) {
			Contributor contributor = new Contributor();
			int t = vn.getText();
			if (t >= 0) {
				contributor.setCreditName(vn.toNormalizedString(t));
				contributors.add(nameIndex, contributor);
				nameIndex++;
			}
		}

		int sequenceIndex = 0;
		ap.selectXPath("//work:contributor/work:contributor-attributes/work:contributor-sequence");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				contributors.get(sequenceIndex).setSequence(vn.toNormalizedString(t));
				sequenceIndex++;
			}
		}

		int roleIndex = 0;
		ap.selectXPath("//work:contributor/work:contributor-attributes/work:contributor-role");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				contributors.get(roleIndex).setRole(vn.toNormalizedString(t));
				roleIndex++;
			}
		}
		return contributors;
	}
}
