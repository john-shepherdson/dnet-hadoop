
package eu.dnetlib.dhp.transformation.xslt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SequenceType;

public class TransformationFunctionProxyTest {
	private VocabularyGroup vocabularies;

	private TransformationFunctionProxy transformationFunctionProxy;

	public void setup() {
		this.transformationFunctionProxy = new TransformationFunctionProxy(vocabularies);
	}

	@Test
	public void shouldGetName() {
		QName actualValue = transformationFunctionProxy.getName();
		String nameSpaceUri = actualValue.getNamespaceURI();
		String prefixName = actualValue.getPrefix();

		assertEquals("http://eu/dnetlib/transform/functionProxy", nameSpaceUri);
		assertEquals("TransformationFunction", prefixName);
	}

	@Test
	public void shouldGetResultType() {
		SequenceType actualValue = transformationFunctionProxy.getResultType();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetArgumentTypes() {
		SequenceType[] actualValue = transformationFunctionProxy.getArgumentTypes();

		// TODO: assert scenario
	}

}
