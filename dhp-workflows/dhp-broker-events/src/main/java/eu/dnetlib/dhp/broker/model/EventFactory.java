
package eu.dnetlib.dhp.broker.model;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;

public class EventFactory {

	private final static String PRODUCER_ID = "OpenAIRE";

	private static final int TTH_DAYS = 365;

	private final static String[] DATE_PATTERNS = {
		"yyyy-MM-dd"
	};

	public static Event newBrokerEvent(final UpdateInfo<?> updateInfo) {

		final long now = new Date().getTime();

		final Event res = new Event();

		final MappedFields map = createMapFromResult(updateInfo);

		final String eventId = calculateEventId(
			updateInfo.getTopicPath(), updateInfo.getTargetDs().getOpenaireId(), updateInfo
				.getTarget()
				.getOpenaireId(),
			updateInfo.getHighlightValueAsString());

		res.setEventId(eventId);
		res.setProducerId(PRODUCER_ID);
		res.setPayload(updateInfo.asBrokerPayload().toJSON());
		res.setMap(map);
		res.setTopic(updateInfo.getTopicPath());
		res.setCreationDate(now);
		res.setExpiryDate(calculateExpiryDate(now));
		res.setInstantMessage(false);

		return res;
	}

	private static MappedFields createMapFromResult(final UpdateInfo<?> updateInfo) {
		final MappedFields map = new MappedFields();

		final OaBrokerMainEntity source = updateInfo.getSource();
		final OaBrokerMainEntity target = updateInfo.getTarget();

		final OaBrokerRelatedDatasource targetDs = updateInfo.getTargetDs();

		map.setTargetDatasourceId(targetDs.getOpenaireId());
		map.setTargetDatasourceName(targetDs.getName());
		map.setTargetDatasourceType(targetDs.getType());

		map.setTargetResultId(target.getOpenaireId());

		final List<String> titles = target.getTitles();
		if (titles.size() > 0) {
			map.setTargetResultTitle(titles.get(0));
		}

		final long date = parseDateTolong(target.getPublicationdate());
		if (date > 0) {
			map.setTargetDateofacceptance(date);
		}

		map.setTargetSubjects(target.getSubjects().stream().map(s -> s.getValue()).collect(Collectors.toList()));
		map.setTargetAuthors(target.getCreators().stream().map(a -> a.getFullname()).collect(Collectors.toList()));

		// PROVENANCE INFO
		map.setTrust(updateInfo.getTrust());
		map.setProvenanceResultId(source.getOpenaireId());

		source
			.getDatasources()
			.stream()
			.filter(ds -> ds.getRelType().equals(BrokerConstants.COLLECTED_FROM_REL))
			.findFirst()
			.ifPresent(ds -> {
				map.setProvenanceDatasourceId(ds.getOpenaireId());
				map.setProvenanceDatasourceName(ds.getName());
				map.setProvenanceDatasourceType(ds.getType());
			});

		return map;
	}

	private static String calculateEventId(final String topic, final String dsId, final String publicationId,
		final String value) {
		return "event-"
			+ DigestUtils.md5Hex(topic).substring(0, 4) + "-"
			+ DigestUtils.md5Hex(dsId).substring(0, 4) + "-"
			+ DigestUtils.md5Hex(publicationId).substring(0, 7) + "-"
			+ DigestUtils.md5Hex(value).substring(0, 5);
	}

	private static long calculateExpiryDate(final long now) {
		return now + TTH_DAYS * 24 * 60 * 60 * 1000;
	}

	private static long parseDateTolong(final String date) {
		if (StringUtils.isBlank(date)) {
			return -1;
		}
		try {
			return DateUtils.parseDate(date, DATE_PATTERNS).getTime();
		} catch (final ParseException e) {
			return -1;
		}
	}

}
