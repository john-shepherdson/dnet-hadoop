
package eu.dnetlib.dhp.utils.saxon;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;

public class NormalizeDate extends AbstractExtensionFunction {

	private static final String[] normalizeDateFormats = {
		"yyyy-MM-dd'T'hh:mm:ss", "yyyy-MM-dd", "yyyy/MM/dd", "yyyy"
	};

	private static final String normalizeOutFormat = "yyyy-MM-dd'T'hh:mm:ss'Z'";

	public static final String BLANK = "";

	@Override
	public String getName() {
		return "normalizeDate";
	}

	@Override
	public Sequence doCall(XPathContext context, Sequence[] arguments) throws XPathException {
		if (arguments == null || arguments.length == 0) {
			return new StringValue(BLANK);
		}
		String s = arguments[0].head().getStringValue();
		return new StringValue(_normalizeDate(s));
	}

	@Override
	public int getMinimumNumberOfArguments() {
		return 0;
	}

	@Override
	public int getMaximumNumberOfArguments() {
		return 1;
	}

	@Override
	public SequenceType[] getArgumentTypes() {
		return new SequenceType[] {
			SequenceType.OPTIONAL_ITEM
		};
	}

	@Override
	public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
		return SequenceType.SINGLE_STRING;
	}

	private String _normalizeDate(String s) {
		final String date = StringUtils.isNotBlank(s) ? s.trim() : BLANK;

		for (String format : normalizeDateFormats) {
			try {
				Date parse = new SimpleDateFormat(format).parse(date);
				String res = new SimpleDateFormat(normalizeOutFormat).format(parse);
				return res;
			} catch (ParseException e) {
			}
		}
		return BLANK;
	}
}
