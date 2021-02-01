
package eu.dnetlib.dhp.transformation;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class TransformationFactory {

	private static final Logger log = LoggerFactory.getLogger(TransformationFactory.class);
	public static final String TRULE_XQUERY = "for $x in collection('/db/DRIVER/TransformationRuleDSResources/TransformationRuleDSResourceType') where $x//RESOURCE_IDENTIFIER/@value = \"%s\" return $x//CODE/text()";

	public static MapFunction<MetadataRecord, MetadataRecord> getTransformationPlugin(
		final Map<String, String> jobArgument, final AggregationCounter counters, final ISLookUpService isLookupService)
		throws DnetTransformationException {

		try {
			final String transformationPlugin = jobArgument.get("transformationPlugin");

			log.info("Transformation plugin required " + transformationPlugin);
			switch (transformationPlugin) {
				case "XSLT_TRANSFORM": {
					final String transformationRuleName = jobArgument.get("transformationRuleTitle");
					if (StringUtils.isBlank(transformationRuleName))
						throw new DnetTransformationException("Missing Parameter transformationRule");
					final VocabularyGroup vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService);

					final String transformationRule = queryTransformationRuleFromIS(
						transformationRuleName, isLookupService);

					final long dateOfTransformation = new Long(jobArgument.get("dateOfTransformation"));
					return new XSLTTransformationFunction(counters, transformationRule, dateOfTransformation,
						vocabularies);

				}
				default:
					throw new DnetTransformationException(
						"transformation plugin does not exists for " + transformationPlugin);

			}

		} catch (Throwable e) {
			throw new DnetTransformationException(e);
		}
	}

	private static String queryTransformationRuleFromIS(final String transformationRuleId,
		final ISLookUpService isLookUpService) throws Exception {
		final String query = String.format(TRULE_XQUERY, transformationRuleId);
		log.info("asking query to IS: " + query);
		List<String> result = isLookUpService.quickSearchProfile(query);

		if (result == null || result.isEmpty())
			throw new DnetTransformationException(
				"Unable to find transformation rule with name: " + transformationRuleId);
		return result.get(0);
	}

}
