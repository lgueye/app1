package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * @author louis.gueye@gmail.com
 */
public class Mapping implements Serializable {

	private Index parent;
	private String type;
	private String definition;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

	public Index getParent() {
		return parent;
	}

	public void setParent(Index parent) {
		this.parent = parent;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Mapping)) return false;

		Mapping mapping = (Mapping) o;

		if (parent != null ? !parent.equals(mapping.parent) : mapping.parent != null) return false;
		if (type != null ? !type.equals(mapping.type) : mapping.type != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = parent != null ? parent.hashCode() : 0;
		result = 31 * result + (type != null ? type.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("type", type)
				.add("definition", definition)
				.toString();
	}
}
