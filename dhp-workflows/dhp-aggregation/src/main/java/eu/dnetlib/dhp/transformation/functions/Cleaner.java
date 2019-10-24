package eu.dnetlib.dhp.transformation.functions;

import eu.dnetlib.dhp.transformation.vocabulary.Term;
import eu.dnetlib.dhp.transformation.vocabulary.Vocabulary;
import net.sf.saxon.s9api.*;
import scala.Serializable;

import java.util.Map;
import java.util.Optional;

public class Cleaner implements ExtensionFunction, Serializable {


    private final  Map<String, Vocabulary> vocabularies;


    public Cleaner(Map<String, Vocabulary> vocabularies) {
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
        return new SequenceType[]
                {
                        SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE),
                        SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ONE)

                };
    }

    @Override
    public XdmValue call(XdmValue[] xdmValues) throws SaxonApiException {
        final String currentValue = xdmValues[0].itemAt(0).getStringValue();
        final String vocabularyName =xdmValues[1].itemAt(0).getStringValue();
        Optional<Term> cleanedValue = vocabularies.get(vocabularyName).getTerms().stream().filter(it -> it.getNativeName().equalsIgnoreCase(currentValue)).findAny();

        return new XdmAtomicValue(cleanedValue.isPresent()?cleanedValue.get().getCode():currentValue);
    }
}
