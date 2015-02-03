package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.File;
import java.io.Serializable;

import com.google.common.base.Objects;

public class Migration implements Serializable {
	private File alias;
	private File source;
	private File target;

	public Migration(File alias, File source, File target) {
		this.setAlias(alias);
		this.setSource(source);
		this.setTarget(target);
	}

	public File getAlias() {
		return alias;
	}

	public void setAlias(File alias) {
		this.alias = alias;
	}

	public File getSource() {
		return source;
	}

	public void setSource(File source) {
		this.source = source;
	}

	public File getTarget() {
		return target;
	}

	public void setTarget(File target) {
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
