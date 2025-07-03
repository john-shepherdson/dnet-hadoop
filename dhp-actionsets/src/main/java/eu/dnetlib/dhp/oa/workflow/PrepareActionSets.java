
package eu.dnetlib.dhp.oa.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import eu.dnetlib.actionmanager.set.RawSet;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.miscutils.datetime.DateUtils;

public class PrepareActionSets {

	private String ACTIONSET_PATH_XQUERY_TEMPLATE = "for $x in collection('/db/DRIVER/ActionManagerSetDSResources/ActionManagerSetDSResourceType') return $x/RESOURCE_PROFILE/BODY/SET[@id = '%s']/@directory/string()";

	protected String execute(String isLookupUrl, String sets) throws Exception {
		Map<String, Object> result = new HashMap<>();

		final List<Map<String, String>> setList = new Gson().fromJson(sets, List.class);
		final String now = DateUtils.now_ISO8601();

		final ISLookUpService lookUpService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final String basePath = lookUpService
			.getResourceProfileByQuery(
				"/RESOURCE_PROFILE[./HEADER/RESOURCE_TYPE/@value='ActionManagerServiceResourceType']//SERVICE_PROPERTIES/PROPERTY[@key='basePath']/@value/string()");

		for (Map<String, String> set : setList) {
			set.put("rawset", RawSet.newInstance().getId());
			set.put("creationDate", now);
			set.put("path", getFullPath(basePath, set, lookUpService));

			// setting the job properties needed to name the rawsets
			result.put(set.get("jobProperty"), set.get("rawset"));
		}

		result.put("sets", setList);
		result.put("actionManagerBasePath", basePath);

		return new Gson().toJson(result);
	}

	private String getFullPath(final String basePath, final Map<String, String> set,
		final ISLookUpService lookUpService) throws ISLookUpException {
		return new Path(basePath + "/" + getPath(set.get("set"), lookUpService) + "/" + set.get("rawset")).toString();
	}

	private String getPath(final String setId, final ISLookUpService lookUpService) throws ISLookUpException {
		return lookUpService.getResourceProfileByQuery(String.format(ACTIONSET_PATH_XQUERY_TEMPLATE, setId));
	}

	public static void main(String[] args) throws Exception {
		CommandLine cmd = parseArguments(args);

		if (cmd != null) {
			String lookupUrl = cmd.getOptionValue("isLookupUrl");
			String sets = cmd.getOptionValue("sets");

			String result = new PrepareActionSets().execute(lookupUrl, sets);
			System.out.println(result);
			System.exit(0);
		}
	}

	private static CommandLine parseArguments(String[] args) {
		Options options = new Options();

		options.addOption(OptionBuilder.withLongOpt("isLookupUrl").hasArg().create("l"));
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
