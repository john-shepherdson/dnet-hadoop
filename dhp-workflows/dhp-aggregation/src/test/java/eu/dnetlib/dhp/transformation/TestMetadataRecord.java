
package eu.dnetlib.dhp.transformation;

import java.io.Serializable;
import java.util.Map;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;

public class TestMetadataRecord implements Serializable {

	private static final long serialVersionUID = -1559828835236900811L;

	/** The D-Net Identifier associated to the record */
	private String id;

	/** The original Identifier of the record */
	private String originalId;

	/** The encoding of the record, should be JSON or XML */
	private String encoding;

	/**
	 * The information about the provenance of the record see @{@link Provenance} for the model of this information
	 */
	private Provenance provenance;

	/** The content of the metadata */
	private String body;

	private Map<String, String> testField;

	/** the date when the record has been stored */
	private Long dateOfCollection;

	/** the date when the record has been stored */
	private Long dateOfTransformation;

	public TestMetadataRecord() {

	}

	public TestMetadataRecord(
		final String originalId,
		final String encoding,
		final Provenance provenance,
		final String body,
		final Long dateOfCollection) {

		this(originalId, encoding, provenance, body, null, dateOfCollection);
	}

	public TestMetadataRecord(
		final String originalId,
		final String encoding,
		final Provenance provenance,
		final String body,
		final Map<String, String> testField,
		final Long dateOfCollection) {

		this.originalId = originalId;
		this.encoding = encoding;
		this.provenance = provenance;
		this.body = body;
		this.testField = testField;
		this.dateOfCollection = dateOfCollection;
		this.id = ModelSupport.generateIdentifier(originalId, this.provenance.getNsPrefix());
	}

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getOriginalId() {
		return this.originalId;
	}

	public void setOriginalId(final String originalId) {
		this.originalId = originalId;
	}

	public String getEncoding() {
		return this.encoding;
	}

	public void setEncoding(final String encoding) {
		this.encoding = encoding;
	}

	public Provenance getProvenance() {
		return this.provenance;
	}

	public void setProvenance(final Provenance provenance) {
		this.provenance = provenance;
	}

	public String getBody() {
		return this.body;
	}

	public void setBody(final String body) {
		this.body = body;
	}

	public Long getDateOfCollection() {
		return this.dateOfCollection;
	}

	public void setDateOfCollection(final Long dateOfCollection) {
		this.dateOfCollection = dateOfCollection;
	}

	public Long getDateOfTransformation() {
		return this.dateOfTransformation;
	}

	public void setDateOfTransformation(final Long dateOfTransformation) {
		this.dateOfTransformation = dateOfTransformation;
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof MetadataRecord)) {
			return false;
		}
		return ((MetadataRecord) o).getId().equalsIgnoreCase(this.id);
	}

	@Override
	public int hashCode() {
		return this.id.hashCode();
	}

	public Map<String, String> getTestField() {
		return this.testField;
	}

	public void setTestField(final Map<String, String> testField) {
		this.testField = testField;
	}
}
