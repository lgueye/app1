package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Migration implements Serializable {
	private String alias;
	private String source;
	private String target;

	public Migration(String alias, String source, String target) {
		this.setAlias(alias);
		this.setSource(source);
		this.setTarget(target);
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Migration)) return false;

		Migration migration = (Migration) o;

		if (alias != null ? !alias.equals(migration.alias) : migration.alias != null) return false;
		if (source != null ? !source.equals(migration.source) : migration.source != null) return false;
		if (target != null ? !target.equals(migration.target) : migration.target != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = alias != null ? alias.hashCode() : 0;
		result = 31 * result + (source != null ? source.hashCode() : 0);
		result = 31 * result + (target != null ? target.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("alias", alias)
				.add("source", source)
				.add("target", target)
				.toString();
	}
}
