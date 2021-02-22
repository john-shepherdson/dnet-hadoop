
package eu.dnetlib.dhp.transformation.xslt;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import net.sf.saxon.s9api.*;
import scala.Serializable;

//import eu.dnetlib.data.collective.transformation.engine.functions.ProcessingException;

// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.Producer;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.common.serialization.LongSerializer;
// import org.apache.kafka.common.serialization.StringSerializer;

public class TransformationFunctionProxy implements ExtensionFunction, Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private final VocabularyGroup vocabularies;
	private final Properties props;

	public TransformationFunctionProxy(final VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;

		/* initialize producer for Kafka events */
		props = new Properties();
		props.put("bootstrap.servers", "events.dnet.openaire.eu:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	}

	@Override
	public QName getName() {
		return new QName("http://eu/dnetlib/transform/functionProxy", "TransformationFunction");
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
		if (r.size() == 0) {
			return new XdmAtomicValue("");
		}
		final String currentValue = xdmValues[0].itemAt(0).getStringValue();
		final String vocabularyName = xdmValues[1].itemAt(0).getStringValue();
		Qualifier cleanedValue = vocabularies.getSynonymAsQualifier(vocabularyName, currentValue);

		return new XdmAtomicValue(
			cleanedValue != null ? cleanedValue.getClassid() : currentValue);
	}

	/**
 	 * normalize values given as an input value by using a vocabulary 
	 * @param aInput - the value as a String
	 * @param aVocabularyName - the name of the vocabulary, which must be known for the vocabulary registry
	 * @return
	 */
	public synchronized String convertString(String aInput, String aVocabularyName){
		List<String> values = new LinkedList<>();

//		Producer<String, String> producer = new KafkaProducer<>(props);

		values.add(aInput);
		try {
			String conversion
// 			log.debug("conversion input: " + aInput);
//			producer.send(new ProducerRecord<String, String>("transformation-vocab", "conversion innput", aInput));

//			String conversionResult = executeSingleValue(aVocabularyName, values);
//			log.debug("conversion result: " + conversionResult);
//			producer.send(new ProducerRecord<String, String>("transformation-vocab", "conversion result", conversionResult));

			return conversionResult;
//		} catch (ProcessingException e) {
		} catch (Exception e) {
//			log.fatal("convert failed for args 'input': " + aInput + " , 'vocabularyName': " + aVocabularyName, e);
//			producer.send(new ProducerRecord<String, String>("transformation-vocab", "convert failed", aVocabularyName));
			throw new IllegalStateException(e);
		}
/*		 catch (KafkaException e) {
			     // For all other exceptions, just abort the transaction and try again.
				 producer.abortTransaction();
		}
*/
//		producer.close();

	}



}
