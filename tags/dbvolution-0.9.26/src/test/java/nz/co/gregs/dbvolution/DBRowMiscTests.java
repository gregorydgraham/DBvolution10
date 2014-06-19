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

package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBRowMiscTests extends AbstractTest{

	public DBRowMiscTests(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void testPrimaryKeyColumnNameForUnspecifiedColumnName(){
		UnspecifiedColumnName unchangedPK = new UnspecifiedColumnName();
		String primaryKeyColumnName = unchangedPK.getPrimaryKeyColumnName();
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}
	
	@Test
	public void testPrimaryKeyColumnNameForSpecifiedColumnName(){
		SpecifiedColumnName changedPK = new SpecifiedColumnName();
		String primaryKeyColumnName = changedPK.getPrimaryKeyColumnName();
		Assert.assertThat(primaryKeyColumnName, is("pk_unique_id"));
	}
	
	@Test
	public void testPrimaryKeyFieldNameForUnspecifiedColumnName(){
		UnspecifiedColumnName unchangedPK = new UnspecifiedColumnName();
		String primaryKeyColumnName = unchangedPK.getPrimaryKeyFieldName();
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}
	
	@Test
	public void testPrimaryKeyFieldNameForSpecifiedColumnName(){
		SpecifiedColumnName changedPK = new SpecifiedColumnName();
		String primaryKeyColumnName = changedPK.getPrimaryKeyFieldName();
		Assert.assertThat(primaryKeyColumnName, is("pkuid"));
	}
	
	public static class UnspecifiedColumnName extends DBRow{
		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		DBInteger pkuid = new DBInteger();
	}
	
	public static class SpecifiedColumnName extends DBRow{
		private static final long serialVersionUID = 1L;
		@DBColumn("pk_unique_id")
		@DBPrimaryKey
		DBInteger pkuid = new DBInteger();
	}
}
