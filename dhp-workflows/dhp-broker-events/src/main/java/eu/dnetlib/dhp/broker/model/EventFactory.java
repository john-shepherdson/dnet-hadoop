
package eu.dnetlib.dhp.broker.model;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class EventFactory {

	private final static String PRODUCER_ID = "OpenAIRE";

	private static final int TTH_DAYS = 365;

	private final static String[] DATE_PATTERNS = {
		"yyyy-MM-dd"
	};

	public static Event newBrokerEvent(final UpdateInfo<?> updateInfo) {

		final long now = new Date().getTime();

		final Event res = new Event();

		final Map<String, Object> map = createMapFromResult(updateInfo);

		final String eventId =
			calculateEventId(updateInfo.getTopicPath(), updateInfo.getTarget().getOriginalId().get(0), updateInfo.getHighlightValueAsString());

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

	private static Map<String, Object> createMapFromResult(final UpdateInfo<?> updateInfo) {
		final Map<String, Object> map = new HashMap<>();

		final Result source = updateInfo.getSource();
		final Result target = updateInfo.getTarget();

		final List<KeyValue> collectedFrom = target.getCollectedfrom();
		if (collectedFrom.size() == 1) {
			map.put("target_datasource_id", collectedFrom.get(0).getKey());
			map.put("target_datasource_name", collectedFrom.get(0).getValue());
		}

		final List<String> ids = target.getOriginalId();
		if (ids.size() > 0) {
			map.put("target_publication_id", ids.get(0));
		}

		final List<StructuredProperty> titles = target.getTitle();
		if (titles.size() > 0) {
			map.put("target_publication_title", titles.get(0));
		}

		final long date = parseDateTolong(target.getDateofacceptance().getValue());
		if (date > 0) {
			map.put("target_dateofacceptance", date);
		}

		final List<StructuredProperty> subjects = target.getSubject();
		if (subjects.size() > 0) {
			map
				.put("target_publication_subject_list", subjects.stream().map(StructuredProperty::getValue).collect(Collectors.toList()));
		}

		final List<Author> authors = target.getAuthor();
		if (authors.size() > 0) {
			map
				.put("target_publication_author_list", authors.stream().map(Author::getFullname).collect(Collectors.toList()));
		}

		// PROVENANCE INFO
		map.put("trust", updateInfo.getTrust());
		final List<KeyValue> sourceCollectedFrom = source.getCollectedfrom();
		if (sourceCollectedFrom.size() == 1) {
			map.put("provenance_datasource_id", sourceCollectedFrom.get(0).getKey());
			map.put("provenance_datasource_name", sourceCollectedFrom.get(0).getValue());
		}
		map.put("provenance_publication_id_list", source.getOriginalId());

		return map;
	}

	private static String calculateEventId(final String topic, final String publicationId, final String value) {
		return "event-"
			+ DigestUtils.md5Hex(topic).substring(0, 6) + "-"
			+ DigestUtils.md5Hex(publicationId).substring(0, 8) + "-"
			+ DigestUtils.md5Hex(value).substring(0, 8);
	}

	private static long calculateExpiryDate(final long now) {
		return now + TTH_DAYS * 24 * 60 * 60 * 1000;
	}

	private static long parseDateTolong(final String date) {
		if (StringUtils.isBlank(date)) { return -1; }
		try {
			return DateUtils.parseDate(date, DATE_PATTERNS).getTime();
		} catch (final ParseException e) {
			return -1;
		}
	}

}
