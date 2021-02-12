
package eu.dnetlib.dhp.transformation.xslt;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.saxon.s9api.*;
import scala.Serializable;

public class DateCleaner implements ExtensionFunction, Serializable {

	private final static List<Pattern> dateRegex = Arrays
		.asList(
			// Y-M-D
			Pattern.compile("(18|19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])", Pattern.MULTILINE),
			// M-D-Y
			Pattern
				.compile(
					"((0[1-9]|1[012])|([1-9]))([- /.])(0[1-9]|[12][0-9]|3[01])([- /.])(18|19|20)?\\d\\d",
					Pattern.MULTILINE),
			// D-M-Y
			Pattern
				.compile(
					"(?:(?:31(/|-|\\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\\1|(?:(?:29|30)(/|-|\\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})|(?:29(/|-|\\.)(?:0?2|(?:Feb))\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))|(?:0?[1-9]|1\\d|2[0-8])(/|-|\\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})",
					Pattern.MULTILINE),
			// Y
			Pattern.compile("(19|20)\\d\\d", Pattern.MULTILINE));

	private final static Pattern incompleteDateRegex = Pattern
		.compile("^((18|19|20)\\d\\d){1}([- \\\\ \\/](0?[1-9]|1[012]))?", Pattern.MULTILINE);

	private final static List<DateTimeFormatter> dformats = Arrays
		.asList(
			DateTimeFormatter
				.ofPattern(
					"[MM-dd-yyyy][MM/dd/yyyy][dd-MM-yy][dd-MMM-yyyy][dd/MMM/yyyy][dd-MMM-yy][dd/MMM/yy][dd-MM-yy][dd/MM/yy][dd-MM-yyyy][dd/MM/yyyy][yyyy-MM-dd][yyyy/MM/dd]",
					Locale.ENGLISH),
			DateTimeFormatter.ofPattern("[dd-MM-yyyy][dd/MM/yyyy]", Locale.ITALIAN));

	public String clean(final String inputDate) {

		Optional<String> cleanedDate = dateRegex
			.stream()
			.map(
				p -> {
					final Matcher matcher = p.matcher(inputDate);
					if (matcher.find())
						return matcher.group(0);
					else
						return null;
				})
			.filter(Objects::nonNull)
			.map(m -> {
				Optional<String> cleanDate = dformats
					.stream()
					.map(f -> {
						try {
							LocalDate parsedDate = LocalDate.parse(m, f);
							if (parsedDate != null)
								return parsedDate.toString();
							else
								return null;
						} catch (Throwable e) {
							return null;
						}
					}

					)
					.filter(Objects::nonNull)
					.findAny();

				return cleanDate.orElse(null);
			})
			.filter(Objects::nonNull)
			.findAny();

		if (cleanedDate.isPresent())
			return cleanedDate.get();

		final Matcher matcher = incompleteDateRegex.matcher(inputDate);
		if (matcher.find()) {
			final Integer year = Integer.parseInt(matcher.group(1));
			final Integer month = Integer.parseInt(matcher.group(4) == null ? "01" : matcher.group(4));
			return String.format("%d-%02d-01", year, month);
		}
		return null;
	}

	@Override
	public QName getName() {
		return new QName("http://eu/dnetlib/trasform/dates", "dateISO");
	}

	@Override
	public SequenceType getResultType() {
		return SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ZERO_OR_ONE);
	}

	@Override
	public SequenceType[] getArgumentTypes() {
		return new SequenceType[] {
			SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ZERO_OR_ONE)
		};
	}

	@Override
	public XdmValue call(XdmValue[] xdmValues) throws SaxonApiException {
		XdmValue r = xdmValues[0];
		if (r.size() == 0) {
			return new XdmAtomicValue("");
		}
		final String currentValue = xdmValues[0].itemAt(0).getStringValue();
		return new XdmAtomicValue(clean(currentValue));
	}
}
