/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.query.nativequery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;

@Entity
//@todo fix problem with rename cache
//@Table(name = OscarWildePoem.TABLE_NAME)


@NamedNativeQueries({
		@NamedNativeQuery(name = "PortiaOscarWildePoem", query = "select _key,_val from OscarWildePoem  where  name='Portia' and author='Oscar Wilde'", resultClass = OscarWildePoem.class),
		@NamedNativeQuery(name = "OscarWildePoemWithParameters", query = "select _key,_val from OscarWildePoem  where  name=?1 and author=?2", resultClass = OscarWildePoem.class)
})
public class OscarWildePoem {

	//public static final String TABLE_NAME = "WILDE_POEM";

	private Long id;
	private String name;
	private String author;
	private byte rating;
	private List<String> mediums = new ArrayList<String>();

	public OscarWildePoem() {
	}

	public OscarWildePoem(Long id, String name, String author, String... mediums) {
		this.id = id;
		this.name = name;
		this.author = author;
		this.mediums = Arrays.asList( mediums );
	}

	public OscarWildePoem(Long id, String name, String author, byte rating) {
		this.id = id;
		this.name = name;
		this.author = author;
		this.rating = rating;
	}

	@Id
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public byte getRating() {
		return rating;
	}

	public void setRating(byte rating) {
		this.rating = rating;
	}

	@ElementCollection
	public List<String> getMediums() {
		return mediums;
	}

	public void setMediums(List<String> medium) {
		this.mediums = medium;
	}

	@Override
	public String toString() {
		return "OscarWildePoem [id=" + id + ", name=" + name + ", author=" + author + ", rating=" + rating + "]";
	}
}
