package eu.dnetlib.dhp.utils.saxon;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;
import org.apache.commons.lang3.StringUtils;

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
        String s1 = arguments[0].head().getStringValue();

        if (arguments.length > 1) {
            String s2 = arguments[1].head().getStringValue();

            return new StringValue(StringUtils.isNotBlank(s1) ? s1 : StringUtils.isNotBlank(s2) ? s2 : "");
        } else {
            return new StringValue(StringUtils.isNotBlank(s1) ? s1 : "");
        }
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
        return new SequenceType[] { SequenceType.OPTIONAL_ITEM };
    }

    @Override
    public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
        return SequenceType.SINGLE_STRING;
    }

}
