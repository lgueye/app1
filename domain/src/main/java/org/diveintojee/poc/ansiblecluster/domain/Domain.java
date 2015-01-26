package org.diveintojee.poc.ansiblecluster.domain;

import com.google.common.base.Objects;
import org.springframework.data.elasticsearch.annotations.Document;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.validation.constraints.Size;
import java.io.Serializable;

/**
 * @author louis.gueye@gmail.com
 */
@Document(indexName = "domains")
@Entity
public class Domain implements Serializable {

    public static final int DESCRIPTION_MAX_SIZE = 200;
    public static final int TITLE_MAX_SIZE = 50;

    @Id
    @GeneratedValue
    private Long id;

    @Column
    @Size(max = TITLE_MAX_SIZE)
    private String title;

    @Column
    @Size(max = DESCRIPTION_MAX_SIZE)
    private String description;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Domain domain = (Domain) o;

        if (description != null ? !description.equals(domain.description) : domain.description != null) return false;
        if (id != null ? !id.equals(domain.id) : domain.id != null) return false;
        if (title != null ? !title.equals(domain.title) : domain.title != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("title", title)
                .add("description", description)
                .toString();
    }
}
