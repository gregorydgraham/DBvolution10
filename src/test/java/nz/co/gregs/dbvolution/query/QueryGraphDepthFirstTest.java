/*
 * Copyright 2014 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.query;

import nz.co.gregs.dbvolution.internal.querygraph.QueryGraph;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class QueryGraphDepthFirstTest extends AbstractTest {

	public QueryGraphDepthFirstTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Override
	public void tearDown(DBDatabase database) throws Exception {
		;
	}

	@Override
	public void setup(DBDatabase database) throws Exception {
		;
	}

	/**
	 * Test of toList method, of class QueryGraphDepthFirst.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testToList() {

		List<DBRow> requiredTables = new ArrayList<DBRow>();
		requiredTables.add(new TableA());
		requiredTables.add(new TableB());
		requiredTables.add(new TableC());

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		optionalTables.add(new TableD());
		optionalTables.add(new TableE());

		QueryGraph graph = new QueryGraph(requiredTables, new ArrayList<BooleanExpression>());
		graph.addOptionalAndConnectToRelevant(optionalTables, new ArrayList<BooleanExpression>());

		List<DBRow> result = graph.toList();

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableD.class, TableE.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAllOptional() {

		List<DBRow> requiredTables = new ArrayList<DBRow>();

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		optionalTables.add(new TableA());
		optionalTables.add(new TableB());
		optionalTables.add(new TableC());
		optionalTables.add(new TableD());
		optionalTables.add(new TableE());

		QueryGraph graph = new QueryGraph(requiredTables, new ArrayList<BooleanExpression>());
		graph.addOptionalAndConnectToRelevant(optionalTables, new ArrayList<BooleanExpression>());

		List<DBRow> result = graph.toList();

		Assert.assertThat(result.size(), is(5));
		Assert.assertThat(result.get(0).getClass(), isOneOf(TableA.class, TableB.class, TableC.class, TableD.class, TableE.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAllOptionalFirstDoesNotStartTheList() {

		List<DBRow> requiredTables = new ArrayList<DBRow>();

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		optionalTables.add(new TableA());
		optionalTables.add(new TableB());
		optionalTables.add(new TableC());
		requiredTables.add(new TableD());
		requiredTables.add(new TableE());

		QueryGraph graph = new QueryGraph(requiredTables, new ArrayList<BooleanExpression>());
		graph.addOptionalAndConnectToRelevant(optionalTables, new ArrayList<BooleanExpression>());

		List<? extends DBRow> result = graph.toList();

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAllRequiredTableWithConditionsDoesStartTheList() {

		List<DBRow> requiredTables = new ArrayList<DBRow>();

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		final TableA tableA = new TableA();
		final TableB tableB = new TableB();
		final TableC tableC = new TableC();
		final TableD tableD = new TableD();
		final TableE tableE = new TableE();
		tableD.uidD.permittedValues(4);
		tableE.uidE.permittedValues(5);

		Assert.assertThat(tableE.hasConditionsSet(), is(true));

		optionalTables.add(tableA);
		optionalTables.add(tableB);
		requiredTables.add(tableC);
		requiredTables.add(tableD);
		requiredTables.add(tableE);

		QueryGraph graph = new QueryGraph(requiredTables, new ArrayList<BooleanExpression>());
		graph.addOptionalAndConnectToRelevant(optionalTables, new ArrayList<BooleanExpression>());

		List<DBRow> result = graph.toList();

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableC.class, TableD.class, TableE.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableC.class, TableD.class, TableE.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableA.class, TableB.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableA.class, TableB.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAllOptionalTableWithConditionsDoesNotStartTheList() {

		List<DBRow> requiredTables = new ArrayList<DBRow>();

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		final TableA tableA = new TableA();
		final TableB tableB = new TableB();
		final TableC tableC = new TableC();
		final TableD tableD = new TableD();
		final TableE tableE = new TableE();
		tableA.uidA.permittedValues(4);
		tableB.uidB.permittedValues(5);

		optionalTables.add(tableA);
		optionalTables.add(tableB);
		requiredTables.add(tableC);
		requiredTables.add(tableD);
		requiredTables.add(tableE);

		QueryGraph graph = new QueryGraph(requiredTables, new ArrayList<BooleanExpression>());
		graph.addOptionalAndConnectToRelevant(optionalTables, new ArrayList<BooleanExpression>());

		List<DBRow> result = graph.toList();

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableC.class, TableD.class, TableE.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableC.class, TableD.class, TableE.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableC.class, TableD.class, TableE.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableA.class, TableB.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableA.class, TableB.class));
	}

	public static class TableA extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger uidA = new DBInteger();
	}

	public static class TableB extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger uidB = new DBInteger();

		@DBForeignKey(TableA.class)
		@DBColumn
		public DBInteger fkA = new DBInteger();
	}

	public static class TableC extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger uidC = new DBInteger();

		@DBForeignKey(TableB.class)
		@DBColumn
		public DBInteger fkB = new DBInteger();

		@DBForeignKey(TableE.class)
		@DBColumn
		public DBInteger fkE = new DBInteger();
	}

	public static class TableD extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger uidD = new DBInteger();

		@DBForeignKey(TableA.class)
		@DBColumn
		public DBInteger fkA = new DBInteger();
	}

	public static class TableE extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger uidE = new DBInteger();

		@DBForeignKey(TableD.class)
		@DBColumn
		public DBInteger fkD = new DBInteger();
	}

}
