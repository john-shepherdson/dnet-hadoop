package eu.dnetlib.doiboost.orcid.xml;

import com.ximpleware.AutoPilot;
import com.ximpleware.EOFException;
import com.ximpleware.EncodingException;
import com.ximpleware.EntityException;
import com.ximpleware.ParseException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import java.util.Arrays;
import java.util.List;

public class XMLRecordParser {

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
    private static final String NS_ERROR = "error";

    public static AuthorData VTDParse(byte[] bytes)
            throws VtdException, EncodingException, EOFException, EntityException, ParseException {
        final VTDGen vg = new VTDGen();
        vg.setDoc(bytes);
        vg.parse(true);
        final VTDNav vn = vg.getNav();
        final AutoPilot ap = new AutoPilot(vn);
        ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
        ap.declareXPathNameSpace(NS_PERSON, NS_PERSON_URL);
        ap.declareXPathNameSpace(NS_DETAILS, NS_DETAILS_URL);
        ap.declareXPathNameSpace(NS_OTHER, NS_OTHER_URL);
        ap.declareXPathNameSpace(NS_RECORD, NS_RECORD_URL);
        ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);

        AuthorData authorData = new AuthorData();
        final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
        if (!errors.isEmpty()) {
            authorData.setErrorCode(errors.get(0));
            return authorData;
        }

        List<VtdUtilityParser.Node> recordNodes =
                VtdUtilityParser.getTextValuesWithAttributes(
                        ap, vn, "//record:record", Arrays.asList("path"));
        if (!recordNodes.isEmpty()) {
            final String oid = (recordNodes.get(0).getAttributes().get("path")).substring(1);
            authorData.setOid(oid);
        } else {
            return null;
        }

        final List<String> names =
                VtdUtilityParser.getTextValue(ap, vn, "//personal-details:given-names");
        if (!names.isEmpty()) {
            authorData.setName(names.get(0));
        }

        final List<String> surnames =
                VtdUtilityParser.getTextValue(ap, vn, "//personal-details:family-name");
        if (!surnames.isEmpty()) {
            authorData.setSurname(surnames.get(0));
        }

        final List<String> creditNames =
                VtdUtilityParser.getTextValue(ap, vn, "//personal-details:credit-name");
        if (!creditNames.isEmpty()) {
            authorData.setCreditName(creditNames.get(0));
        }
        return authorData;
    }
}
