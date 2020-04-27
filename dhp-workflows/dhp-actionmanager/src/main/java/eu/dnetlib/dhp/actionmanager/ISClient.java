
package eu.dnetlib.dhp.actionmanager;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;
import eu.dnetlib.actionmanager.set.ActionManagerSet;
import eu.dnetlib.actionmanager.set.ActionManagerSet.ImpactTypes;
import eu.dnetlib.dhp.actionmanager.partition.PartitionActionSetsByPayloadTypeJob;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ISClient implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PartitionActionSetsByPayloadTypeJob.class);

	private static final String INPUT_ACTION_SET_ID_SEPARATOR = ",";

	private ISLookUpService isLookup;

	public ISClient(String isLookupUrl) {
		isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);
	}

	public List<String> getLatestRawsetPaths(String setIds) {

		List<String> ids = Lists
			.newArrayList(
				Splitter
					.on(INPUT_ACTION_SET_ID_SEPARATOR)
					.omitEmptyStrings()
					.trimResults()
					.split(setIds));

		return ids
			.stream()
			.map(id -> getSet(isLookup, id))
			.map(as -> as.getPathToLatest())
			.collect(Collectors.toCollection(ArrayList::new));
	}

	private ActionManagerSet getSet(ISLookUpService isLookup, final String setId) {

		final String q = "for $x in collection('/db/DRIVER/ActionManagerSetDSResources/ActionManagerSetDSResourceType') "
			+ "where $x//SET/@id = '"
			+ setId
			+ "' return $x";

		try {
			final String basePath = getBasePathHDFS(isLookup);
			final String setProfile = isLookup.getResourceProfileByQuery(q);
			return getActionManagerSet(basePath, setProfile);
		} catch (ISLookUpException | ActionManagerException e) {
			throw new RuntimeException("Error accessing Sets, using query: " + q);
		}
	}

	private ActionManagerSet getActionManagerSet(final String basePath, final String profile)
		throws ActionManagerException {
		final SAXReader reader = new SAXReader();
		final ActionManagerSet set = new ActionManagerSet();

		try {
			final Document doc = reader.read(new StringReader(profile));

			set.setId(doc.valueOf("//SET/@id").trim());
			set.setName(doc.valueOf("//SET").trim());
			set.setImpact(ImpactTypes.valueOf(doc.valueOf("//IMPACT").trim()));
			set
				.setLatest(
					doc.valueOf("//RAW_SETS/LATEST/@id"),
					doc.valueOf("//RAW_SETS/LATEST/@creationDate"),
					doc.valueOf("//RAW_SETS/LATEST/@lastUpdate"));
			set.setDirectory(doc.valueOf("//SET/@directory"));
			final List expiredNodes = doc.selectNodes("//RAW_SETS/EXPIRED");
			if (expiredNodes != null) {
				for (int i = 0; i < expiredNodes.size(); i++) {
					Element ex = (Element) expiredNodes.get(i);
					set
						.addExpired(
							ex.attributeValue("id"),
							ex.attributeValue("creationDate"),
							ex.attributeValue("lastUpdate"));
				}
			}

			final StringBuilder sb = new StringBuilder();
			sb.append(basePath);
			sb.append("/");
			sb.append(doc.valueOf("//SET/@directory"));
			sb.append("/");
			sb.append(doc.valueOf("//RAW_SETS/LATEST/@id"));
			set.setPathToLatest(sb.toString());

			return set;
		} catch (Exception e) {
			throw new ActionManagerException("Error creating set from profile: " + profile, e);
		}
	}

	private String getBasePathHDFS(ISLookUpService isLookup) throws ActionManagerException {
		return queryServiceProperty(isLookup, "basePath");
	}

	private String queryServiceProperty(ISLookUpService isLookup, final String propertyName)
		throws ActionManagerException {
		final String q = "for $x in /RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='ActionManagerServiceResourceType'] return $x//SERVICE_PROPERTIES/PROPERTY[./@ key='"
			+ propertyName
			+ "']/@value/string()";
		log.debug("quering for service property: " + q);
		try {
			final List<String> value = isLookup.quickSearchProfile(q);
			return Iterables.getOnlyElement(value);
		} catch (ISLookUpException e) {
			String msg = "Error accessing service profile, using query: " + q;
			log.error(msg, e);
			throw new ActionManagerException(msg, e);
		} catch (NoSuchElementException e) {
			String msg = "missing service property: " + propertyName;
			log.error(msg, e);
			throw new ActionManagerException(msg, e);
		} catch (IllegalArgumentException e) {
			String msg = "found more than one service property: " + propertyName;
			log.error(msg, e);
			throw new ActionManagerException(msg, e);
		}
	}
}
