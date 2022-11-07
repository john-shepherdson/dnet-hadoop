
package eu.dnetlib.dhp.transformation.xslt;

import static eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction.QNAME_BASE_URI;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import net.sf.saxon.s9api.*;

public class DateCleaner implements ExtensionFunction, Serializable {

	@Override
	public QName getName() {
		return new QName(QNAME_BASE_URI + "/dateISO", "dateISO");
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

	// for backward compatibility with the existing unit tests
	public String clean(String date) {
		return GraphCleaningFunctions.cleanDate(date);
	}
}
