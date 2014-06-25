/*
 * Copyright 2014 gregorygraham.
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

import java.util.ArrayList;
import java.util.List;
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
 * @author gregorygraham
 */
public class QueryGraphDepthFirstTest extends AbstractTest {

	public QueryGraphDepthFirstTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of toList method, of class QueryGraphDepthFirst.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testToList() {
		System.out.println("toList");

		List<DBRow> requiredTables = new ArrayList<DBRow>();
		requiredTables.add(new TableA());
		requiredTables.add(new TableB());
		requiredTables.add(new TableC());

		List<DBRow> optionalTables = new ArrayList<DBRow>();
		optionalTables.add(new TableD());
		optionalTables.add(new TableE());

		QueryGraph graph = new QueryGraph(database, requiredTables, new ArrayList<BooleanExpression>(), new QueryOptions());
		graph.addOptionalAndConnectToRelevant(database, optionalTables, new ArrayList<BooleanExpression>(), new QueryOptions());

		List<DBRow> result = graph.toList(TableA.class);
		for (DBRow dBRow : result) {
			System.out.println("" + dBRow.getClass().getSimpleName());
		}

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableD.class, TableE.class));

		result = graph.toList(TableC.class);
		for (DBRow dBRow : result) {
			System.out.println("" + dBRow.getClass().getSimpleName());
		}

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableD.class, TableE.class));

		result = graph.toList(TableD.class);
		for (DBRow dBRow : result) {
			System.out.println("" + dBRow.getClass().getSimpleName());
		}

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableD.class, TableE.class));

		result = graph.toList(TableE.class);
		for (DBRow dBRow : result) {
			System.out.println("" + dBRow.getClass().getSimpleName());
		}

		Assert.assertThat(result.get(0).getClass(), isOneOf(TableD.class, TableE.class));
		Assert.assertThat(result.get(1).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(2).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(3).getClass(), isOneOf(TableA.class, TableB.class, TableC.class));
		Assert.assertThat(result.get(4).getClass(), isOneOf(TableD.class, TableE.class));
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
