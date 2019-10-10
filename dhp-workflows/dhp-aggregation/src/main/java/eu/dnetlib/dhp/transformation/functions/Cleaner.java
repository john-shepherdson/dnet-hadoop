package eu.dnetlib.dhp.transformation.functions;

import net.sf.saxon.s9api.*;

public class Cleaner implements ExtensionFunction {

    @Override
    public QName getName() {
        return new QName("http://eu/dnetlib/trasform/extension", "clean");
    }

    @Override
    public SequenceType getResultType() {
        return SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE);
    }

    @Override
    public SequenceType[] getArgumentTypes() {
        return new SequenceType[]
                {
                        SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE)
                };
    }

    @Override
    public XdmValue call(XdmValue[] xdmValues) throws SaxonApiException {
        final String currentValue = xdmValues[0].itemAt(0).getStringValue();
        return new XdmAtomicValue("cleaned"+currentValue);
    }
}
