
package eu.dnetlib.dhp.utils.saxon;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.commons.lang3.StringUtils;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;

public class PickFirst extends AbstractExtensionFunction {

	@Override
	public String getName() {
		return "pickFirst";
	}

	@Override
	public Sequence doCall(XPathContext context, Sequence[] arguments) throws XPathException {
		if (arguments == null | arguments.length == 0) {
			return new StringValue("");
		}

		final String s1 = getValue(arguments[0]);
		final String s2 = getValue(arguments[1]);

		final String value = isNotBlank(s1) ? s1 : isNotBlank(s2) ? s2 : "";
		return new StringValue(value);
	}

	private String getValue(final Sequence arg) throws XPathException {
		if (arg != null) {
			final Item item = arg.head();
			if (item != null) {
				return item.getStringValue();
			}
		}
		return "";
	}

	@Override
	public int getMinimumNumberOfArguments() {
		return 0;
	}

	@Override
	public int getMaximumNumberOfArguments() {
		return 2;
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
}
