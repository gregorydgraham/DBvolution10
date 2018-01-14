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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.datatypes.DBEnumTest.StringEnumTable.StringEnumType;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.junit.Assert;
import org.junit.Test;

public class DBEnumTest extends AbstractTest {

	public DBEnumTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void createRecordUsingEnum() {
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setValue(IntEnum.MOVEMENT_CANCELLATION_REQUEST); // nested class imported
		row.recordType.setValue(IntEnum.MOVEMENT_CANCELLATION_REQUEST); // explicit reference to nested class

		String sqlFragment = row.recordType.toSQLString(database.getDefinition());
		assertThat(sqlFragment, is("3"));
	}

	@Test
	public void createRecordUsingLiteral() {
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setLiteralValue(
				IntEnum.MOVEMENT_CANCELLATION_REQUEST.getCode());

		String sqlFragment = row.recordType.toSQLString(database.getDefinition());
		assertThat(sqlFragment, is("3"));
	}

	@Test
	public void filterRecordUsingEnum() {
		IntegerEnumTable rowExemplar = new IntegerEnumTable();
		rowExemplar.recordType.permittedValues(
				IntEnum.MOVEMENT_REQUEST_RECORD,
				IntEnum.SHIPPING_MANIFEST_RECORD);

		String sqlFragment = database.getDBQuery(rowExemplar).getSQLForQuery();
		assertThat(sqlFragment.toLowerCase(), containsString("c_5 in ( 2, 1)"));
	}

	@Test
	public void filterRecordUsingLiteral() {
		IntegerEnumTable rowExemplar = new IntegerEnumTable();
		rowExemplar.recordType.permittedValues(
				IntEnum.MOVEMENT_REQUEST_RECORD.getCode(),
				IntEnum.SHIPPING_MANIFEST_RECORD.getCode());

		String sqlFragment = database.getDBQuery(rowExemplar).getSQLForQuery();
		assertThat(sqlFragment.toLowerCase(), containsString("c_5 in ( 2, 1)"));
	}

	@Test
	public void processIntegerRecord() throws SQLException {
		final IntegerEnumTable integerTableExemplar = new IntegerEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumTable(1, IntEnum.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumTable(2, IntEnum.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumTable(4, IntEnum.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.recordType.permittedValues(
					IntEnum.MOVEMENT_CANCELLATION_REQUEST,
					IntEnum.MOVEMENT_REQUEST_RECORD,
					IntEnum.SHIPPING_MANIFEST_RECORD);
			List<IntegerEnumTable> rows = database.get(integerTableExemplar);

			for (IntegerEnumTable row : rows) {
				if (row.uid_202.getValue().intValue() == 1) {
					assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
				}
				if (row.uid_202.getValue().intValue() == 2) {
					assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
				}
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
		}
	}

	@Test
	public void processIntegerRecordWithValue() throws SQLException {
		final IntegerEnumWithDefinedValuesTable integerTableExemplar = new IntegerEnumWithDefinedValuesTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumWithDefinedValuesTable(1, IntEnum.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumWithDefinedValuesTable(2, IntEnum.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumWithDefinedValuesTable(4, IntEnum.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.uid_202.permittedValues(
					0, null);
			List<IntegerEnumWithDefinedValuesTable> rows = database.get(integerTableExemplar);

			for (IntegerEnumWithDefinedValuesTable row : rows) {
				assertThat(row.request.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
				assertThat(row.manifest.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
				assertThat(row.cancel.enumValue(), is(IntEnum.MOVEMENT_CANCELLATION_REQUEST));
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
		}
	}

	@Test
	public void integerEnumPermittedRange() throws SQLException {
		final IntegerEnumTable integerTableExemplar = new IntegerEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumTable(1, IntEnum.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumTable(2, IntEnum.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumTable(4, IntEnum.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.recordType.permittedRange(
					IntEnum.MOVEMENT_REQUEST_RECORD, null);
			List<IntegerEnumTable> rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), isOneOf(IntEnum.SHIPPING_MANIFEST_RECORD, IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.excludedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.getValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.numberValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(0));

			integerTableExemplar.recordType.excludedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			integerTableExemplar.recordType.excludedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			integerTableExemplar.recordType.excludedValues(
					null, IntEnum.SHIPPING_MANIFEST_RECORD, IntEnum.MOVEMENT_CANCELLATION_REQUEST);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));
			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.longValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD.code));
			}

		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
		}
	}

	@Test
	public void stringEnumPermittedRange() throws SQLException {
		final StringEnumTable stringTableExemplar = new StringEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(stringTableExemplar);
		database.createTable(stringTableExemplar);
		try {
			database.insert(
					new StringEnumTable(1, StringEnumType.MOVEMENT_REQUEST_RECORD),
					new StringEnumTable(2, StringEnumType.SHIPPING_MANIFEST_RECORD),
					new StringEnumTable(4, StringEnumType.MOVEMENT_REQUEST_RECORD));

			stringTableExemplar.recordType.permittedRange(
					StringEnumType.MOVEMENT_REQUEST_RECORD, null);
			List<StringEnumTable> rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
			}

			stringTableExemplar.recordType.permittedRange(
					null, StringEnumType.MOVEMENT_REQUEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(1));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(StringEnumType.SHIPPING_MANIFEST_RECORD));
			}

			stringTableExemplar.recordType.permittedRangeInclusive(
					null, StringEnumType.MOVEMENT_REQUEST_RECORD);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(3));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), isOneOf(StringEnumType.SHIPPING_MANIFEST_RECORD, StringEnumType.MOVEMENT_REQUEST_RECORD));
			}

			stringTableExemplar.recordType.permittedRangeExclusive(
					null, StringEnumType.MOVEMENT_REQUEST_RECORD);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(1));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(StringEnumType.SHIPPING_MANIFEST_RECORD));
			}

			stringTableExemplar.recordType.excludedRange(
					null, StringEnumType.MOVEMENT_REQUEST_RECORD);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));

			stringTableExemplar.recordType.excludedRange(
					null, StringEnumType.MOVEMENT_REQUEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
			}

			stringTableExemplar.recordType.permittedRangeInclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(1));

			stringTableExemplar.recordType.excludedRangeInclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));

			stringTableExemplar.recordType.excludedRangeInclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));

			for (StringEnumTable row : rows) {
				assertThat(row.recordType.getValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD.literalValue));
				assertThat(row.recordType.enumValue().getCode(), is(StringEnumType.MOVEMENT_REQUEST_RECORD.getCode()));
				assertThat(row.recordType.enumValue().getDisplayName(), is(IntEnum.MOVEMENT_REQUEST_RECORD.getDisplayName()));
			}

			stringTableExemplar.recordType.permittedRangeExclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(0));

			stringTableExemplar.recordType.excludedRangeExclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(3));

			stringTableExemplar.recordType.excludedRangeExclusive(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD.literalValue);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(3));

			stringTableExemplar.recordType.excludedValues(
					null, StringEnumType.SHIPPING_MANIFEST_RECORD, StringEnumType.MOVEMENT_CANCELLATION_REQUEST);
			rows = database.get(stringTableExemplar);

			assertThat(rows.size(), is(2));
			for (StringEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
			}

		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(stringTableExemplar);
		}
	}

	@Test
	public void integerPermittedRange() throws SQLException {
		final IntegerEnumTable integerTableExemplar = new IntegerEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumTable(1, IntEnum.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumTable(2, IntEnum.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumTable(4, IntEnum.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.recordType.permittedRange(
					IntEnum.MOVEMENT_REQUEST_RECORD.code, null);
			List<IntegerEnumTable> rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue().code, is(IntEnum.MOVEMENT_REQUEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), isOneOf(IntEnum.SHIPPING_MANIFEST_RECORD, IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.excludedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.getValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.numberValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(0));

			integerTableExemplar.recordType.excludedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			integerTableExemplar.recordType.excludedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			integerTableExemplar.recordType.excludedValues(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.code, IntEnum.MOVEMENT_CANCELLATION_REQUEST.code);
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));
			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.longValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD.code));
			}

		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
		}
	}

	@Test
	public void longPermittedRange() throws SQLException {
		final IntegerEnumTable integerTableExemplar = new IntegerEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumTable(1, IntEnum.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumTable(2, IntEnum.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumTable(4, IntEnum.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.recordType.permittedRange(
					IntEnum.MOVEMENT_REQUEST_RECORD.getLong(), null);
			List<IntegerEnumTable> rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue().code, is(IntEnum.MOVEMENT_REQUEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), isOneOf(IntEnum.SHIPPING_MANIFEST_RECORD, IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD));
			}

			integerTableExemplar.recordType.excludedRange(
					null, IntEnum.MOVEMENT_REQUEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD));
			}

			integerTableExemplar.recordType.permittedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));

			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.getValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.numberValue().longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
				assertThat(row.recordType.longValue(), is(IntEnum.SHIPPING_MANIFEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(0));

			integerTableExemplar.recordType.excludedRangeExclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(3));

			integerTableExemplar.recordType.excludedRangeInclusive(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));

			integerTableExemplar.recordType.excludedValues(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong(), IntEnum.MOVEMENT_CANCELLATION_REQUEST.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(2));
			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.longValue(), is(IntEnum.MOVEMENT_REQUEST_RECORD.code));
			}

			integerTableExemplar.recordType.permittedValues(
					null, IntEnum.SHIPPING_MANIFEST_RECORD.getLong(), IntEnum.MOVEMENT_CANCELLATION_REQUEST.getLong());
			rows = database.get(integerTableExemplar);

			assertThat(rows.size(), is(1));
			for (IntegerEnumTable row : rows) {
				assertThat(row.recordType.longValue(), is(0L + IntEnum.SHIPPING_MANIFEST_RECORD.code));
			}

		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
		}
	}

	@Test
	public void processStringRecord() throws SQLException {
		final StringEnumTable stringTableExemplar = new StringEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(stringTableExemplar);
		database.createTable(stringTableExemplar);
		try {
			database.insert(
					new StringEnumTable(1, StringEnumType.MOVEMENT_REQUEST_RECORD),
					new StringEnumTable(2, StringEnumType.SHIPPING_MANIFEST_RECORD),
					new StringEnumTable(4, StringEnumType.MOVEMENT_REQUEST_RECORD));

			stringTableExemplar.recordType.permittedValues(
					StringEnumType.MOVEMENT_CANCELLATION_REQUEST,
					StringEnumType.MOVEMENT_REQUEST_RECORD,
					StringEnumType.SHIPPING_MANIFEST_RECORD);
			List<StringEnumTable> rows = database.get(stringTableExemplar);

			Assert.assertThat(rows.size(), is(3));
			Assert.assertThat(rows.get(0).movereq.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
			for (StringEnumTable row : rows) {
				if (row.uid_202.getValue() == 1) {
					assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
				} else if (row.uid_202.getValue().intValue() == 2) {
					assertThat(row.recordType.enumValue(), is(StringEnumType.SHIPPING_MANIFEST_RECORD));
				} else if (row.uid_202.getValue().intValue() == 4) {
					assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
				} else {
					throw new RuntimeException("Unknown Row Found");
				}
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(stringTableExemplar);

		}
	}

	@Test
	public void correctlyConvertsLongToIntegerEnum() {
		// warm up enum type
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setValue(IntEnum.SHIPPING_MANIFEST_RECORD);

		// do test
		long code = IntEnum.MOVEMENT_CANCELLATION_REQUEST.code;
		row.recordType.setLiteralValue(code);
		assertThat(row.recordType.enumValue(), is(IntEnum.MOVEMENT_CANCELLATION_REQUEST));
	}

	@Test
	public void operatorsWorkWithStringRecord() throws SQLException {
		final StringEnumTable stringTableExemplar = new StringEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(stringTableExemplar);
		database.createTable(stringTableExemplar);
		database.insert(
				new StringEnumTable(1, StringEnumType.MOVEMENT_REQUEST_RECORD),
				new StringEnumTable(2, StringEnumType.SHIPPING_MANIFEST_RECORD),
				new StringEnumTable(4, StringEnumType.MOVEMENT_REQUEST_RECORD));

		stringTableExemplar.recordType.permittedValues(
				StringEnumType.MOVEMENT_CANCELLATION_REQUEST,
				StringEnumType.MOVEMENT_REQUEST_RECORD);
		List<StringEnumTable> rows = database.get(stringTableExemplar);

		Assert.assertThat(rows.size(), is(2));

		stringTableExemplar.recordType.excludedValues(
				StringEnumType.SHIPPING_MANIFEST_RECORD,
				StringEnumType.MOVEMENT_CANCELLATION_REQUEST
		);
		rows = database.get(stringTableExemplar);

		Assert.assertThat(rows.size(), is(2));

		stringTableExemplar.recordType.permittedRangeInclusive(
				StringEnumType.MOVEMENT_REQUEST_RECORD,
				null
		);
		rows = database.get(stringTableExemplar);

		Assert.assertThat(rows.size(), is(2));

		stringTableExemplar.recordType.excludedRangeInclusive(
				StringEnumType.MOVEMENT_CANCELLATION_REQUEST,
				StringEnumType.SHIPPING_MANIFEST_RECORD
		);
		rows = database.get(stringTableExemplar);
		Assert.assertThat(rows.size(), is(2));

		stringTableExemplar.recordType.excludedPattern(StringEnumType.SHIPPING_MANIFEST_RECORD.literalValue);
		rows = database.get(stringTableExemplar);
		Assert.assertThat(rows.size(), is(2));

		stringTableExemplar.recordType.permittedPattern(StringEnumType.MOVEMENT_REQUEST_RECORD.literalValue);
		rows = database.get(stringTableExemplar);
		Assert.assertThat(rows.size(), is(2));

		ArrayList<StringEnumType> arrayList = new ArrayList<>();
		arrayList.add(StringEnumType.SHIPPING_MANIFEST_RECORD);
		arrayList.add(StringEnumType.MOVEMENT_CANCELLATION_REQUEST);
		stringTableExemplar.recordType.excludedValuesIgnoreCase(arrayList);
		rows = database.get(stringTableExemplar);
		Assert.assertThat(rows.size(), is(2));

		arrayList.clear();
		arrayList.add(StringEnumType.MOVEMENT_REQUEST_RECORD);
		stringTableExemplar.recordType.permittedValuesIgnoreCase(arrayList);
		rows = database.get(stringTableExemplar);
		Assert.assertThat(rows.size(), is(2));
	}

	public static class IntegerEnumTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_202")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		@DBColumn("c_5")
		public DBIntegerEnum<IntEnum> recordType = new DBIntegerEnum<>();

		@DBColumn("ordinal3")
		public DBIntegerEnum<IntEnum> justOrdinal3 = new DBIntegerEnum<IntEnum>(IntegerExpression.value(2));

		public IntegerEnumTable() {
		}

		public IntegerEnumTable(Integer uid, IntEnum recType) {
			this.uid_202.setValue(uid);
			this.recordType.setValue(recType);
		}
	}

	/**
	 * Valid values for {@link #recordType}.
	 *
	 * <p>
	 * Nested class to make it obvious which table the enum is for
	 */
	public static enum IntEnum implements DBEnumValue<Long> {

		SHIPPING_MANIFEST_RECORD(1, "Shipping Manifest Record"),
		MOVEMENT_REQUEST_RECORD(2, "Movement Request Record"),
		MOVEMENT_CANCELLATION_REQUEST(3, "Movement Cancellation Request");

		private long code;
		private String displayName;

		private IntEnum(int code, String displayName) {
			this.code = code;
			this.displayName = displayName;
		}

		@Override
		public Long getCode() {
			return code;
		}

		public Long getLong() {
			return code;
		}

		public String getDisplayName() {
			return displayName;
		}

		public static IntEnum valueOfCode(DBInteger code) {
			return valueOfCode(code == null ? null : code.getValue().longValue());
		}

		public static IntEnum valueOfCode(Long code) {
			if (code == null) {
				return null;
			}
			for (IntEnum recordType : values()) {
				if (recordType.getCode().equals(code)) {
					return recordType;
				}
			}
			throw new IllegalArgumentException("Invalid " + IntEnum.class.getSimpleName() + " code: " + code);
		}
	}

	public static class IntegerEnumWithDefinedValuesTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_202")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		public DBIntegerEnum<IntEnum> manifest = new DBIntegerEnum<>(1);
		public DBIntegerEnum<IntEnum> request = new DBIntegerEnum<>(2L);
		public DBIntegerEnum<IntEnum> cancel = new DBIntegerEnum<>(IntEnum.MOVEMENT_CANCELLATION_REQUEST);

		public IntegerEnumWithDefinedValuesTable() {
		}

		public IntegerEnumWithDefinedValuesTable(Integer uid, IntEnum recType) {
			this.uid_202.setValue(uid);
		}
	}

	public static class StringEnumTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_203")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		@DBColumn("c_5")
		public DBStringEnum<StringEnumType> recordType = new DBStringEnum<>();

		@DBColumn
		public DBStringEnum<StringEnumType> movereq = new DBStringEnum<>(StringExpression.value(StringEnumType.MOVEMENT_REQUEST_RECORD.literalValue));

		public StringEnumTable() {
		}

		public StringEnumTable(Integer uid, StringEnumType recType) {
			this.uid_202.setValue(uid);
			this.recordType.setValue(recType);
		}

		/**
		 * Valid values for {@link #recordType}
		 */
		// Nested class to make it obvious which table the enum is for
		public static enum StringEnumType implements DBEnumValue<String> {

			SHIPPING_MANIFEST_RECORD("MANRECORD", "Shipping Manifest Record"),
			MOVEMENT_REQUEST_RECORD("MOVEREQ", "Movement Request Record"),
			MOVEMENT_CANCELLATION_REQUEST("CANCREQ", "Movement Cancellation Request");
			private final String literalValue;
			private final String displayName;

			private StringEnumType(String code, String displayName) {
				this.literalValue = code;
				this.displayName = displayName;
			}

			@Override
			public String getCode() {
				return literalValue;
			}

			public String getDisplayName() {
				return displayName;
			}

			public static StringEnumType valueOfCode(DBString code) {
				return valueOfCode(code == null ? null : code.stringValue());
			}

			public static StringEnumType valueOfCode(String code) {
				if (code == null) {
					return null;
				}
				for (StringEnumType recordType : values()) {
					if (recordType.getCode().equals(code)) {
						return recordType;
					}
				}
				throw new IllegalArgumentException("Invalid " + StringEnumType.class.getSimpleName() + " code: " + code);
			}
		}
	}

	public static class DBGenericEnumImpl<E extends Enum<E> & DBEnumValue<String>> extends DBEnum<E, String> {

		public static final long serialVersionUID = 1l;

		public DBGenericEnumImpl() {
		}

		public DBGenericEnumImpl(StringExpression stringExpression) {
			super(stringExpression);
		}

		@Override
		protected void validateLiteralValue(E enumValue) {
			String localValue = enumValue.getCode();
			if (localValue != null) {
				if (!(localValue instanceof String)) {
					String enumMethodRef = enumValue.getClass().getName() + "." + enumValue.name() + ".getLiteralValue()";
					String literalValueTypeRef = localValue.getClass().getName();
					throw new IncompatibleClassChangeError("Enum literal type is not valid: "
							+ enumMethodRef + " returned a " + literalValueTypeRef + ", which is not valid for a " + this.getClass().getSimpleName());
				}
			}
		}

		@Override
		protected void setValueFromStandardStringEncoding(String encodedValue) {
			setValue(encodedValue);
		}

		@Override
		public String getSQLDatatype() {
			return new DBString().getSQLDatatype();
		}

		@Override
		protected String getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
			return resultSet.getString(fullColumnName);
		}

		@Override
		public ColumnProvider getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
			return new StringColumn(row, this);
		}
	}

	public static class GenericEnumTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_203")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		@DBColumn("c_5")
		public DBGenericEnumImpl<GenericEnumType> recordType = new DBGenericEnumImpl<>();

		@DBColumn
		public DBGenericEnumImpl<GenericEnumType> movereq = new DBGenericEnumImpl<>(StringExpression.value(GenericEnumType.MOVEMENT_REQUEST_RECORD.literalValue));

		public GenericEnumTable() {
		}

		public GenericEnumTable(Integer uid, GenericEnumType recType) {
			this.uid_202.setValue(uid);
			this.recordType.setValue(recType);
		}
	}

	/**
	 * Valid values for {@link #recordType}
	 */
	public static enum GenericEnumType implements DBEnumValue<String> {

		SHIPPING_MANIFEST_RECORD("MANRECORD", "Shipping Manifest Record"),
		MOVEMENT_REQUEST_RECORD("MOVEREQ", "Movement Request Record"),
		MOVEMENT_CANCELLATION_REQUEST("CANCREQ", "Movement Cancellation Request");
		private final String literalValue;
		private final String displayName;

		private GenericEnumType(String code, String displayName) {
			this.literalValue = code;
			this.displayName = displayName;
		}

		@Override
		public String getCode() {
			return literalValue;
		}

		public String getDisplayName() {
			return displayName;
		}

		public static GenericEnumType valueOfCode(DBString code) {
			return valueOfCode(code == null ? null : code.stringValue());
		}

		public static GenericEnumType valueOfCode(String code) {
			if (code == null) {
				return null;
			}
			for (GenericEnumType recordType : values()) {
				if (recordType.getCode().equals(code)) {
					return recordType;
				}
			}
			throw new IllegalArgumentException("Invalid " + StringEnumType.class.getSimpleName() + " code: " + code);
		}
	}

	@Test
	public void processGenericRecord() throws SQLException {
		final GenericEnumTable genericTableExemplar = new GenericEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(genericTableExemplar);
		database.createTable(genericTableExemplar);
		try {
			database.insert(
					new GenericEnumTable(1, GenericEnumType.MOVEMENT_REQUEST_RECORD),
					new GenericEnumTable(2, GenericEnumType.SHIPPING_MANIFEST_RECORD),
					new GenericEnumTable(4, GenericEnumType.MOVEMENT_REQUEST_RECORD));

			genericTableExemplar.recordType.permittedValues(
					GenericEnumType.MOVEMENT_CANCELLATION_REQUEST,
					GenericEnumType.MOVEMENT_REQUEST_RECORD,
					GenericEnumType.SHIPPING_MANIFEST_RECORD);
			List<GenericEnumTable> rows = database.get(genericTableExemplar);

			Assert.assertThat(rows.size(), is(3));
			Assert.assertThat(rows.get(0).movereq.enumValue(), is(GenericEnumType.MOVEMENT_REQUEST_RECORD));
			for (GenericEnumTable row : rows) {
				if (row.uid_202.getValue() == 1) {
					assertThat(row.recordType.enumValue(), is(GenericEnumType.MOVEMENT_REQUEST_RECORD));
				} else if (row.uid_202.getValue().intValue() == 2) {
					assertThat(row.recordType.enumValue(), is(GenericEnumType.SHIPPING_MANIFEST_RECORD));
				} else if (row.uid_202.getValue().intValue() == 4) {
					assertThat(row.recordType.enumValue(), is(GenericEnumType.MOVEMENT_REQUEST_RECORD));
				} else {
					throw new RuntimeException("Unknown Row Found");
				}
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(genericTableExemplar);

		}
	}
}
