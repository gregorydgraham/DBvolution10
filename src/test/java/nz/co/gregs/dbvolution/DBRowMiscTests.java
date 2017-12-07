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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.exceptions.UndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBRowMiscTests extends AbstractTest {

	public DBRowMiscTests(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testPrimaryKeyColumnNameForUnspecifiedColumnName() {
		UnspecifiedColumnName unchangedPK = new UnspecifiedColumnName();
		String primaryKeyColumnName = unchangedPK.getPrimaryKeyColumnNames().get(0);
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}

	@Test
	public void testPrimaryKeyColumnNameForSpecifiedColumnName() {
		SpecifiedColumnName changedPK = new SpecifiedColumnName();
		String primaryKeyColumnName = changedPK.getPrimaryKeyColumnNames().get(0);
		Assert.assertThat(primaryKeyColumnName, is("pk_unique_id"));
	}

	@Test
	public void testPrimaryKeyFieldNameForUnspecifiedColumnName() {
		UnspecifiedColumnName unchangedPK = new UnspecifiedColumnName();
		String primaryKeyColumnName = unchangedPK.getPrimaryKeyFieldName().get(0);
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}

	@Test
	public void testPrimaryKeyFieldNameForSpecifiedColumnName() {
		SpecifiedColumnName changedPK = new SpecifiedColumnName();
		String primaryKeyColumnName = changedPK.getPrimaryKeyFieldName().get(0);
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}

	@Test
	public void testSetPrimaryKey() {
		SpecifiedColumnName changedPK = new SpecifiedColumnName();
		changedPK.setPrimaryKey(2);
		Assert.assertThat(changedPK.pkuid.intValue(), is(2));
	}

	@Test(expected = UndefinedPrimaryKeyException.class)
	public void testSetPrimaryKeyWithoutPrimaryKeyThrowsUndefinedPrimaryKeyException() {
		WithoutPrimaryKey changedPK = new WithoutPrimaryKey();
		changedPK.setPrimaryKey("2");
	}

	public static class UnspecifiedColumnName extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		DBInteger pkuid = new DBInteger();
	}

	public static class SpecifiedColumnName extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn("pk_unique_id")
		@DBPrimaryKey
		DBInteger pkuid = new DBInteger();
	}

	public static class WithoutPrimaryKey extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn("pk_unique_id")
		DBInteger pkuid = new DBInteger();
	}
}
