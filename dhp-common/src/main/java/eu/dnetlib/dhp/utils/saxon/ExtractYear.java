package eu.dnetlib.dhp.utils.saxon;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class ExtractYear extends AbstractExtensionFunction {

    private static final String[] dateFormats = { "yyyy-MM-dd", "yyyy/MM/dd" };

    @Override
    public String getName() {
        return "extractYear";
    }

    @Override
    public Sequence doCall(XPathContext context, Sequence[] arguments) throws XPathException {
        if (arguments == null | arguments.length == 0) {
            return new StringValue("");
        }
        final Item item = arguments[0].head();
        if (item == null) {
            return new StringValue("");
        }
        return new StringValue(_year(item.getStringValue()));
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
        return new SequenceType[] { SequenceType.OPTIONAL_ITEM };
    }

    @Override
    public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
        return SequenceType.SINGLE_STRING;
    }

    private String _year(String s) {
        Calendar c = new GregorianCalendar();
        for (String format : dateFormats) {
            try {
                c.setTime(new SimpleDateFormat(format).parse(s));
                String year = String.valueOf(c.get(Calendar.YEAR));
                return year;
            } catch (ParseException e) {}
        }
        return "";
    }
}
