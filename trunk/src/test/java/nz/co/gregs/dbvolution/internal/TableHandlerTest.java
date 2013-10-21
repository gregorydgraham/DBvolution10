package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.junit.Test;

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
    public void tableNameUnsetGivenAnnotatedNonDBRowClass() {
        TableHandler handler = new TableHandler(MyAnnotatedNonDBRow.class);
        assertThat(handler.getTableName(), is(nullValue()));
    }

    @Test
    public void tableNameUnsetGivenNonAnnotatedNonDBRowClass() {
        TableHandler handler = new TableHandler(MyNonAnnotatedNonDBRow.class);
        assertThat(handler.getTableName(), is(nullValue()));
    }

    @DBTableName("foo")
    @SuppressWarnings("serial")
    public static class MyAnnotatedDBRow extends DBRow {
    }

    @SuppressWarnings("serial")
    public static class MyNonAnnotatedDBRow extends DBRow {
    }

    @DBTableName("foo")
    public static class MyAnnotatedNonDBRow {
    }

    public static class MyNonAnnotatedNonDBRow {
    }
}
