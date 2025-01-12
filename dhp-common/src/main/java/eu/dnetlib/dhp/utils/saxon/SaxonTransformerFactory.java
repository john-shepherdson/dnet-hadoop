
package eu.dnetlib.dhp.utils.saxon;

import java.io.StringReader;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.TransformerFactoryImpl;

public class SaxonTransformerFactory {

	private SaxonTransformerFactory() {
	}

	/**
	 * Creates the index record transformer from the given XSLT
	 *
	 * @param xslt
	 * @return
	 * @throws TransformerException
	 */
	public static Transformer newInstance(final String xslt) throws TransformerException {

		final TransformerFactoryImpl factory = new TransformerFactoryImpl();
		final Configuration conf = factory.getConfiguration();
		conf.registerExtensionFunction(new ExtractYear());
		conf.registerExtensionFunction(new NormalizeDate());
		conf.registerExtensionFunction(new PickFirst());

		return factory.newTransformer(new StreamSource(new StringReader(xslt)));
	}
}
