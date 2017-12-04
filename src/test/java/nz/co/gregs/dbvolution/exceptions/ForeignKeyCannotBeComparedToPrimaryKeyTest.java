/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.exceptions;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class ForeignKeyCannotBeComparedToPrimaryKeyTest {

	public ForeignKeyCannotBeComparedToPrimaryKeyTest() {
	}

	@Test(expected = ForeignKeyCannotBeComparedToPrimaryKey.class)
	public void testExceptionIsThrown() {
		TableA tableA = new TableA();
		TableE tableE = new TableE();

		List<DBExpression> foreignKeyExpressionsTo = tableE.getForeignKeyExpressionsTo(tableA);
	}

	@Test()
	public void testExceptionIsNotThrown() {
		TableA tableA = new TableA();
		TableAString tableAString = new TableAString();
		TableB tableB = new TableB();

		List<DBExpression> foreignKeyExpressionsTo = tableB.getForeignKeyExpressionsTo(tableA);
		Assert.assertThat(foreignKeyExpressionsTo.size(), is(1));

		foreignKeyExpressionsTo = tableB.getForeignKeyExpressionsTo(tableAString);
		Assert.assertThat(foreignKeyExpressionsTo.size(), is(1));
	}

	@Test(expected = ForeignKeyCannotBeComparedToPrimaryKey.class)
	public void testExceptionIsThrownForStringToo() {
		TableAString tableAString = new TableAString();
		TableE tableE = new TableE();

		List<DBExpression> foreignKeyExpressionsTo = tableE.getForeignKeyExpressionsTo(tableAString);
	}

	public static class TableA extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		public DBInteger apk = new DBInteger();

		public TableA() {
			super();
		}
	}

	public static class TableAString extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		public DBString apk = new DBString();

		public TableAString() {
			super();
		}
	}

	public static class TableE extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		public DBInteger bpk = new DBInteger();

		@DBColumn
		@DBForeignKey(TableA.class)
		DBString afk = new DBString();

		@DBColumn
		@DBForeignKey(TableAString.class)
		DBInteger aStringfk = new DBInteger();
	}

	public static class TableB extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		DBInteger bpk = new DBInteger();

		@DBColumn
		@DBForeignKey(TableA.class)
		public DBInteger afk = new DBInteger();

		@DBColumn
		@DBForeignKey(TableAString.class)
		public DBString aStringfk = new DBString();

		public TableB() {
			super();
		}
	}

}
