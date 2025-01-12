
package eu.dnetlib.pace.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import eu.dnetlib.pace.config.Type;

/**
 * The schema is composed by field definitions (FieldDef). Each field has a type, a name, and an associated compare algorithm.
 */
public class FieldDef implements Serializable {

	public final static String PATH_SEPARATOR = "/";

	private String name;

	private String path;

	private Type type;

	private boolean overrideMatch;

	/**
	 * Sets maximum size for the repeatable fields in the model. -1 for unbounded size.
	 */
	private int size = -1;

	/**
	 * Sets maximum length for field values in the model. -1 for unbounded length.
	 */
	private int length = -1;

	private HashSet<String> filter;

	private boolean sorted;

	public boolean isSorted() {
		return sorted;
	}

	private String clean;

	private String infer;

	private String inferenceFrom;

	public FieldDef() {
	}

	public String getInferenceFrom() {
		return inferenceFrom;
	}

	public void setInferenceFrom(final String inferenceFrom) {
		this.inferenceFrom = inferenceFrom;
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public List<String> getPathList() {
		return Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(getPath()));
	}

	public Type getType() {
		return type;
	}

	public void setType(final Type type) {
		this.type = type;
	}

	public boolean isOverrideMatch() {
		return overrideMatch;
	}

	public void setOverrideMatch(final boolean overrideMatch) {
		this.overrideMatch = overrideMatch;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public HashSet<String> getFilter() {
		return filter;
	}

	public void setFilter(HashSet<String> filter) {
		this.filter = filter;
	}

	public boolean getSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}

	public String getClean() {
		return clean;
	}

	public void setClean(String clean) {
		this.clean = clean;
	}

	public String getInfer() {
		return infer;
	}

	public void setInfer(String infer) {
		this.infer = infer;
	}

	@Override
	public String toString() {
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return null;
		}
	}

}
