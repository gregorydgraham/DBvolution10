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

import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class ForeignKeyCannotBeComparedToPrimaryKeyTest {
	
	public ForeignKeyCannotBeComparedToPrimaryKeyTest() {
	}

	@Test(expected = ForeignKeyCannotBeComparedToPrimaryKey.class)
	public void testSomeMethod() {
		TableA tableA = new TableA();
		TableB tableB = new TableB();
		List<DBExpression> foreignKeyExpressionsTo = tableB.getForeignKeyExpressionsTo(tableA);
	}
	
	public class TableA extends DBRow{
		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		DBInteger apk = new DBInteger();
	}
	
	public class TableB extends DBRow{
		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		DBInteger bpk = new DBInteger();

		@DBColumn
		@DBForeignKey(TableA.class)
		DBString afk = new DBString();
	}
	
}
