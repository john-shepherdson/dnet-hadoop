package eu.dnetlib.dhp.transformation.xslt;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import net.sf.saxon.s9api.QName;

@RunWith(MockitoJUnitRunner.class)
public class TransformationFunctionProxyTest {
	@Mock
	private VocabularyGroup vocabularies;

	private TransformationFunctionProxy transformationFunctionProxy;

	@Before
	public void setup() {
		this.transformationFunctionProxy = new TransformationFunctionProxy(vocabularies);
	}

	@Test
	public void shouldGetName() {
		QName actualValue = transformationFunctionProxy.getName();
		String prefixName = actualValue.getPrefix();

		assertEquals("http://eu/dnetlib/transform/functionProxy", actualValue.uri);
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

	@Test
	public void shouldCall() {
		// TODO: initialize args
		XdmValue[] xdmValues;

		XdmValue actualValue = transformationFunctionProxy.call(xdmValues);

		// TODO: assert scenario
	}

	@Test
	public void shouldConvertString() {
		// TODO: initialize args
		String aInput;
		String aVocabularyName;

		String actualValue = transformationFunctionProxy.convertString(aInput, aVocabularyName);

		// TODO: assert scenario
	}
}
