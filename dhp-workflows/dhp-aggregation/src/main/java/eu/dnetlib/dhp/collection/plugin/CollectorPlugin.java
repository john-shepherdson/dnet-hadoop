
package eu.dnetlib.dhp.collection.plugin;

import java.util.stream.Stream;

import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;

public interface CollectorPlugin {

	enum NAME {
		oai, other;

		public enum OTHER_NAME {
			mdstore_mongodb_dump, mdstore_mongodb
		}

	}

	Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException;

}
