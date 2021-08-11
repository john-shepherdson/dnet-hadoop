
package eu.dnetlib.dhp.oa.graph.reflections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class ReflectionTest {

	private final Cleaner cleaner = new Cleaner();

	@Test
	void testObject() throws Exception {
		final Publication pub = new Publication();
		pub.setTitle("openaire guidelines");
		pub.getAuthors().add(new Author("Michele Artini", new Prop("aa-001", "orcid")));
		pub.getAuthors().add(new Author("Claudio Atzori", new Prop("aa-002", "orcid")));
		pub.getAuthors().add(new Author("Alessia Bardi", new Prop("aa-003", "orcid")));
		pub.getSubjects().add(new Prop("infrastructures", "keyword"));
		pub.getSubjects().add(new Prop("digital libraries", "keyword"));

		cleaner.clean(pub);

		System.out.println(pub);

		assertEquals("OPENAIRE GUIDELINES", pub.getTitle());

		assertEquals("MICHELE ARTINI", pub.getAuthors().get(0).getName());
		assertEquals("CLAUDIO ATZORI", pub.getAuthors().get(1).getName());
		assertEquals("ALESSIA BARDI", pub.getAuthors().get(2).getName());

		assertEquals("dnet:aa-001", pub.getAuthors().get(0).getId().getId());
		assertEquals("dnet:aa-002", pub.getAuthors().get(1).getId().getId());
		assertEquals("dnet:aa-003", pub.getAuthors().get(2).getId().getId());
		assertEquals("dnet:orcid", pub.getAuthors().get(0).getId().getName());
		assertEquals("dnet:orcid", pub.getAuthors().get(1).getId().getName());
		assertEquals("dnet:orcid", pub.getAuthors().get(2).getId().getName());

		assertEquals("dnet:infrastructures", pub.getSubjects().get(0).getId());
		assertEquals("dnet:keyword", pub.getSubjects().get(0).getName());
		assertEquals("dnet:digital libraries", pub.getSubjects().get(1).getId());
		assertEquals("dnet:keyword", pub.getSubjects().get(1).getName());
	}

}

class Cleaner {

	public void clean(final Object o) throws IllegalArgumentException, IllegalAccessException {
		if (isPrimitive(o)) {
			return;
		} else if (isIterable(o.getClass())) {
			for (final Object elem : (Iterable<?>) o) {
				clean(elem);
			}
		} else if (hasMapping(o)) {
			mapObject(o);
		} else {
			for (final Field f : o.getClass().getDeclaredFields()) {
				f.setAccessible(true);
				final Object val = f.get(o);
				if (isPrimitive(val)) {
					f.set(o, cleanValue(f.get(o)));
				} else if (hasMapping(val)) {
					mapObject(val);
				} else {
					clean(f.get(o));
				}
			}
		}
	}

	private boolean hasMapping(final Object o) {
		return o.getClass() == Prop.class;
	}

	private void mapObject(final Object o) {
		if (o.getClass() == Prop.class) {
			((Prop) o).setId("dnet:" + ((Prop) o).getId());
			((Prop) o).setName("dnet:" + ((Prop) o).getName());
		}

	}

	private Object cleanValue(final Object o) {
		if (o.getClass() == String.class) {
			return ((String) o).toUpperCase();
		} else {
			return o;
		}

	}

	private boolean isIterable(final Class<?> cl) {
		return Iterable.class.isAssignableFrom(cl);
	}

	private boolean isPrimitive(final Object o) {
		return o.getClass() == String.class;
	}
}

class Publication {

	private String title;
	private final List<Author> authors = new ArrayList<>();
	private final List<Prop> subjects = new ArrayList<>();

	public String getTitle() {
		return title;
	}

	public void setTitle(final String title) {
		this.title = title;
	}

	public List<Author> getAuthors() {
		return authors;
	}

	public List<Prop> getSubjects() {
		return subjects;
	}

	@Override
	public String toString() {
		return String.format("Publication [title=%s, authors=%s, subjects=%s]", title, authors, subjects);
	}

}

class Prop {

	private String id;
	private String name;

	public Prop(final String id, final String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return String.format("Prop [id=%s, name=%s]", id, name);
	}

}

class Author {

	private String name;
	private Prop id;

	public Author(final String name, final Prop id) {
		this.name = name;
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public Prop getId() {
		return id;
	}

	public void setId(final Prop id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return String.format("Author [name=%s, id=%s]", name, id);
	}

}
