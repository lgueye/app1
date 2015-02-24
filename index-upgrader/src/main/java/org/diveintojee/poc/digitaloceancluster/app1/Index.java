package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Set;

/**
 * @author louis.gueye@gmail.com
 */
public class Index implements Serializable, Comparable<Index> {

	private String alias;
	private String version;
	private String settings;
	private Set<Mapping> mappings = Sets.newHashSet();

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getSettings() {
		return settings;
	}

	public void setSettings(String settings) {
		this.settings = settings;
	}

	public Set<Mapping> getMappings() {
		return mappings;
	}

    @Override
    public int compareTo(Index index) {
        return getAlias().compareTo(index.getAlias()) + getVersion().compareTo(index.getVersion());
    }

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Index)) return false;

		Index index = (Index) o;

		if (alias != null ? !alias.equals(index.alias) : index.alias != null) return false;
		if (version != null ? !version.equals(index.version) : index.version != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = alias != null ? alias.hashCode() : 0;
		result = 31 * result + (version != null ? version.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("alias", alias)
				.add("version", version)
				.add("settings", settings)
				.add("mappings", mappings)
				.toString();
	}

	public void addMapping(Mapping mapping) {
		mapping.setParent(this);
		this.mappings.add(mapping);
	}

    public String getName() {
        return alias + "_" + version;
    }
}
