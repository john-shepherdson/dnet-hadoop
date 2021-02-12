
package eu.dnetlib.dhp.transformation.xslt;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import net.sf.saxon.s9api.*;
import scala.Serializable;

public class Cleaner implements ExtensionFunction, Serializable {

	private final VocabularyGroup vocabularies;

	public Cleaner(final VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;
	}

	@Override
	public QName getName() {
		return new QName("http://eu/dnetlib/trasform/extension", "clean");
	}

	@Override
	public SequenceType getResultType() {
		return SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE_OR_MORE);
	}

	@Override
	public SequenceType[] getArgumentTypes() {
		return new SequenceType[] {
			SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ZERO_OR_MORE),
			SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE)
		};
	}

	@Override
	public XdmValue call(XdmValue[] xdmValues) throws SaxonApiException {
		XdmValue r = xdmValues[0];
		if (r.size() == 0){
			return new XdmAtomicValue("");
		}
		final String currentValue = xdmValues[0].itemAt(0).getStringValue();
		final String vocabularyName = xdmValues[1].itemAt(0).getStringValue();
		Qualifier cleanedValue = vocabularies.getSynonymAsQualifier(vocabularyName, currentValue);

		return new XdmAtomicValue(
			cleanedValue != null ? cleanedValue.getClassid() : currentValue);
	}
}
