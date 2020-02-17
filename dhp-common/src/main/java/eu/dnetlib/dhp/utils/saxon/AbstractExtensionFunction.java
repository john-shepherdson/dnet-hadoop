package eu.dnetlib.dhp.utils.saxon;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.lib.ExtensionFunctionCall;
import net.sf.saxon.lib.ExtensionFunctionDefinition;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.trans.XPathException;

public abstract class AbstractExtensionFunction extends ExtensionFunctionDefinition {

    public static String DEFAULT_SAXON_EXT_NS_URI = "http://www.d-net.research-infrastructures.eu/saxon-extension";

    public abstract String getName();
    public abstract Sequence doCall(XPathContext context, Sequence[] arguments) throws XPathException;

    @Override
    public StructuredQName getFunctionQName() {
        return new StructuredQName("dnet", DEFAULT_SAXON_EXT_NS_URI, getName());
    }

    @Override
    public ExtensionFunctionCall makeCallExpression() {
        return new ExtensionFunctionCall() {
            @Override
            public Sequence call(XPathContext context, Sequence[] arguments) throws XPathException {
                return doCall(context, arguments);
            }
        };
    }

}