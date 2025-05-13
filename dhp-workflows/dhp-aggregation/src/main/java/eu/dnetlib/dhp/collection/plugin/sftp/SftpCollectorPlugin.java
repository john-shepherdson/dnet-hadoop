
package eu.dnetlib.dhp.collection.plugin.sftp;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class SftpCollectorPlugin implements CollectorPlugin {

	private static final int SFTP_PORT = 22;

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final String url = api.getBaseUrl();

		final String username = Optional
			.ofNullable(api.getParams().get("username"))
			.filter(StringUtils::isNotBlank)
			.orElseThrow(() -> new CollectorException("Param 'username' is null or empty"));

		final int port = Optional
			.ofNullable(api.getParams().get("port"))
			.filter(StringUtils::isNotBlank)
			.map(s -> NumberUtils.toInt(s, SFTP_PORT))
			.orElse(SFTP_PORT);

		final boolean recursive = Optional
			.ofNullable(api.getParams().get("recursive"))
			.filter(StringUtils::isNotBlank)
			.map(BooleanUtils::toBoolean)
			.orElse(false);

		final Set<String> extensions = Optional
			.ofNullable(api.getParams().get("extensions"))
			.filter(StringUtils::isNotBlank)
			.map(s -> Sets.newHashSet(Splitter.on(",").omitEmptyStrings().trimResults().split(s)))
			.orElseThrow(() -> new CollectorException("Param 'extensions' is null or empty"));

		final String fromDate = api.getParams().get("fromDate");
		if ((fromDate != null) && !fromDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
			throw new CollectorException("Invalid date (YYYY-MM-DD): " + fromDate);
		}

		final Iterator<String> iter;

		final String authMethod = api.getParams().get("authMethod");
		final String password = api.getParams().get("password");
		final String privateKeyPath = api.getParams().get("privateKeyPath");

		if ("key".equalsIgnoreCase(authMethod) && StringUtils.isNotBlank(privateKeyPath)) {
			iter = new SftpIteratorWithAuthenticationKey(url, port, username, recursive, extensions, fromDate,
				privateKeyPath);
		} else if (!"key".equalsIgnoreCase(authMethod) && StringUtils.isNotBlank(password)) {
			iter = new SftpIteratorWithPassword(url, port, username, recursive, extensions, fromDate, password);
		} else {
			throw new CollectorException(
				"Invalid authentication params, verify the parameters: authMethod, password and privateKeyPath");
		}

		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
	}

}
