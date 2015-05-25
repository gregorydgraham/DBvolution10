package nz.co.gregs.dbvolution.internal.properties;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.junit.Test;

/**
 * Note: this unit test tests some aspects of inheritance of the annotation. It
 * turns out that inheritance of annotations is quite a complex business and
 * must be defined on an per-annotation-basis. The following provides some more
 * information:
 * <ul>
 * <li>
 * http://www.jroller.com/melix/entry/the_truth_about_annotations_inheritance
 * <li>
 * http://eclipse.org/aspectj/doc/next/adk15notebook/annotations.html#annotation-inheritance
 * </ul>
 */
@SuppressWarnings("serial")
public class TableHandlerTest {

	@Test
	public void acceptsStandardAnnotatedClassAsTable() {
		TableHandler handler = new TableHandler(MyAnnotatedDBRow.class);
		assertThat(handler.isTable(), is(true));
	}

	@Test
	public void acceptsStandardNonAnnotatedClassAsTable() {
		TableHandler handler = new TableHandler(MyNonAnnotatedDBRow.class);
		assertThat(handler.isTable(), is(true));
	}

	@Test
	public void rejectsAnnotatedNonDBRowClassAsNotATable() {
		TableHandler handler = new TableHandler(MyAnnotatedNonDBRow.class);
		assertThat(handler.isTable(), is(false));
		assertThat(handler.getTableName(), is(nullValue()));
	}

	@Test
	public void rejectsNonAnnotatedNonDBRowClassAsNotATable() {
		TableHandler handler = new TableHandler(MyNonAnnotatedNonDBRow.class);
		assertThat(handler.isTable(), is(false));
		assertThat(handler.getTableName(), is(nullValue()));
	}

	@Test
	public void tableNameSetGivenAnnotatedDBRowClass() {
		TableHandler handler = new TableHandler(MyAnnotatedDBRow.class);
		assertThat(handler.getTableName(), is("foo"));
	}

	@Test
	public void tableNameDefaultedGivenNonAnnotatedDBRowClass() {
		TableHandler handler = new TableHandler(MyNonAnnotatedDBRow.class);
		assertThat(handler.getTableName(), is("MyNonAnnotatedDBRow"));
	}

	@Test
	public void tableNameUnsetGivenAnnotatedNonDBRowClass() {
		TableHandler handler = new TableHandler(MyAnnotatedNonDBRow.class);
		assertThat(handler.getTableName(), is(nullValue()));
	}

	@Test
	public void tableNameUnsetGivenNonAnnotatedNonDBRowClass() {
		TableHandler handler = new TableHandler(MyNonAnnotatedNonDBRow.class);
		assertThat(handler.getTableName(), is(nullValue()));
	}

	@Test
	public void tableNameInheritedGivenNonAnnotatedSubclassOfAnnotatedDBRowClass() {
		TableHandler handler = new TableHandler(NonAnnotatedSubclassOfAnnotatedDBRow.class);
		assertThat(handler.getTableName(), is("foo"));
	}

	@Test
	public void tableNameInheritedGivenNonAnnotatedSubclassOfNonAnnotatedDBRowClass() {
		TableHandler handler = new TableHandler(NonAnnotatedSubclassOfNonAnnotatedDBRow.class);
		assertThat(handler.getTableName(), is("MyNonAnnotatedDBRow"));
	}

	@Test
	public void tableNameOverriddenGivenAnnotatedSubclassOfNonAnnotatedDBRowClass() {
		TableHandler handler = new TableHandler(AnnotatedSubclassOfAnnotatedDBRow.class);
		assertThat(handler.getTableName(), is("bar"));
	}

	@DBTableName("foo")
	public static class MyAnnotatedDBRow extends DBRow {
	}

	public static class MyNonAnnotatedDBRow extends DBRow {
	}

	@DBTableName("foo")
	public static class MyAnnotatedNonDBRow {
	}

	public static class MyNonAnnotatedNonDBRow {
	}

	public static class NonAnnotatedSubclassOfAnnotatedDBRow extends MyAnnotatedDBRow {

	}

	public static class NonAnnotatedSubclassOfNonAnnotatedDBRow extends MyNonAnnotatedDBRow {

	}

	@DBTableName("bar")
	public static class AnnotatedSubclassOfAnnotatedDBRow extends MyAnnotatedDBRow {

	}
}
