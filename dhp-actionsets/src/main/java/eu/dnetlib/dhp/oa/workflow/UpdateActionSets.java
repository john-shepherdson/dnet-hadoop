
package eu.dnetlib.dhp.oa.workflow;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.*;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.google.gson.Gson;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.ISRegistryServiceFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.enabling.is.registry.rmi.ISRegistryException;
import eu.dnetlib.enabling.is.registry.rmi.ISRegistryService;
import eu.dnetlib.miscutils.datetime.DateUtils;

public class UpdateActionSets {
	public static final String SET_UPDATED = "updated";
	public static final String SET_ENABLED = "enabled";
	public static final String SETS_ENV_ATTRIBUTE = "sets";

	final ISLookUpService lookUpService;

	final ISRegistryService registryService;

	public UpdateActionSets(String isLookupUrl, String isRegistryUrl) {
		this.lookUpService = ISLookupClientFactory.getLookUpService(isLookupUrl);
		this.registryService = ISRegistryServiceFactory.getRegistryServiceService(isRegistryUrl);
	}

	String execute(String isets) throws Exception {

		final Gson gson = new Gson();
		final List<Map<String, String>> sets = gson.fromJson(isets, List.class);

		final String lastUpdate = DateUtils.now_ISO8601();
		for (Map<String, String> set : sets) {

			// update only the enabled sets that were not yet updated.
			if (isEnabled(set) && !isSetUpdated(set)) {
				System.out.println("updating set: " + set.toString());
				addLatestRawSet(set, lastUpdate);
			} else {
				System.out.println("skip set update: " + set.toString());
			}
		}

		return gson.toJson(sets);
	}

	private boolean isEnabled(final Map<String, String> set) {
		return set.containsKey(SET_ENABLED) && set.get(SET_ENABLED).equals("true");
	}

	private boolean isSetUpdated(final Map<String, String> set) {
		return set.containsKey(SET_UPDATED) && set.get(SET_UPDATED).equals("true");
	}

	public void addLatestRawSet(final Map<String, String> set, final String lastUpdate)
		throws ISLookUpException, DocumentException, ISRegistryException {
		final String q = "for $x in collection('/db/DRIVER/ActionManagerSetDSResources/ActionManagerSetDSResourceType') where $x//SET/@id = '"
			+ set.get("set")
			+ "' return $x";
		try {
			final String profile = lookUpService.getResourceProfileByQuery(q);
			final Document doc = new SAXReader().read(new StringReader(profile));
			final String profId = doc.valueOf("//RESOURCE_IDENTIFIER/@value");
			final Element latest = (Element) doc.selectSingleNode("//RAW_SETS/LATEST");
			final Element expired = ((Element) doc.selectSingleNode("//RAW_SETS")).addElement("EXPIRED");

			for (Object o : latest.attributes()) {
				final Attribute a = (Attribute) o;
				expired.addAttribute(a.getName(), a.getValue());
			}

			latest.addAttribute("id", set.get("rawset"));
			latest.addAttribute("creationDate", set.get("creationDate"));
			latest.addAttribute("lastUpdate", lastUpdate);

			registryService.updateProfile(profId, doc.asXML(), "ActionManagerSetDSResourceType");

			// mark the set profile update as completed
			set.put(SET_UPDATED, "true");
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLine cmd = parseArguments(args);

		if (cmd != null) {
			String lookupUrl = cmd.getOptionValue("isLookupUrl");
			String registryUrl = cmd.getOptionValue("isRegistryUrl");
			String sets = cmd.getOptionValue("sets");

			String result = new UpdateActionSets(lookupUrl, registryUrl).execute(sets);
			System.out.println(result);
			System.exit(0);
		}
	}

	private static CommandLine parseArguments(String[] args) {
		Options options = new Options();

		options.addOption(OptionBuilder.withLongOpt("isLookupUrl").hasArg().create("l"));
		options.addOption(OptionBuilder.withLongOpt("isRegistryUrl").hasArg().create("r"));
		options.addOption(OptionBuilder.withLongOpt("sets").hasArg().create("s"));

		CommandLineParser parser = new BasicParser();
		HelpFormatter formatter = new HelpFormatter();

		try {
			return parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("Error parsing arguments: " + e.getMessage());
			formatter.printHelp("Main", options);
			return null;
		}
	}
}
