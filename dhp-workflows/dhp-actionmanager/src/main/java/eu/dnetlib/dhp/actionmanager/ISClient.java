
package eu.dnetlib.dhp.actionmanager;

import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.actionmanager.rmi.ActionManagerException;
import eu.dnetlib.actionmanager.set.ActionManagerSet;
import eu.dnetlib.actionmanager.set.ActionManagerSet.ImpactTypes;
import eu.dnetlib.dhp.actionmanager.partition.PartitionActionSetsByPayloadTypeJob;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class ISClient implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PartitionActionSetsByPayloadTypeJob.class);

	private static final String INPUT_ACTION_SET_ID_SEPARATOR = ",";

	private final ISLookUpService isLookup;

	public ISClient(String isLookupUrl) {
		isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);
	}

	public List<String> getLatestRawsetPaths(String setIds) {

		final Set<String> ids = Sets
			.newHashSet(
				Splitter
					.on(INPUT_ACTION_SET_ID_SEPARATOR)
					.omitEmptyStrings()
					.trimResults()
					.split(setIds));
		try {
			final String basePath = getBasePathHDFS(isLookup);

			// <SET id="..." directory="..." latest="xxx"/>
			final String xquery = "for $x in collection('/db/DRIVER/ActionManagerSetDSResources/ActionManagerSetDSResourceType') "
				+
				"return <SET id='{$x//SET/@id/string()}' directory='{$x//SET/@directory/string()}' latest='{$x//LATEST/@id/string()}'/>";
			return Optional
				.ofNullable(isLookup.quickSearchProfile(xquery))
				.map(
					sets -> sets
						.stream()
						.map(set -> parseSetInfo(set))
						.filter(t -> ids.contains(t.getLeft()))
						.map(t -> buildDirectory(basePath, t))
						.collect(Collectors.toList()))
				.orElseThrow(() -> new IllegalStateException("empty set list"));
		} catch (ActionManagerException | ISLookUpException e) {
			throw new IllegalStateException("unable to query ActionSets info from the IS");
		}
	}

	private Triple<String, String, String> parseSetInfo(String set) {
		try {
			Document doc = new SAXReader().read(new StringReader(set));
			return Triple
				.of(
					doc.valueOf("//SET/@id"),
					doc.valueOf("//SET/@directory"),
					doc.valueOf("//SET/@latest"));
		} catch (DocumentException e) {
			throw new IllegalStateException(e);
		}
	}

	private String buildDirectory(String basePath, Triple<String, String, String> t) {
		return Joiner.on("/").join(basePath, t.getMiddle(), t.getRight());
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
