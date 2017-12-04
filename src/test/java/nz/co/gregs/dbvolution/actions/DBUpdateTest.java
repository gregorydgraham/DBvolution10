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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalUpdateOfUndefinedRowException;
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
public class DBUpdateTest extends AbstractTest {

	public DBUpdateTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getUpdates method, of class DBUpdate.
	 *
	 * @throws java.lang.Exception
	 */
	@Test(expected = AccidentalUpdateOfUndefinedRowException.class)
	public void testGetUpdates() throws Exception {
		Marque marque = new Marque();
		marque.name.setValue("Not a Marque");
		DBUpdate.getUpdates(marque);
	}

	@Test
	public void updateUsingAllColumnsAndRevertTest() throws SQLException {
		LinkCarCompanyAndLogo example = new LinkCarCompanyAndLogo();
		example.fkCarCompany.setValue(1);
		example.fkCompanyLogo.setValue(1);
		database.getDBTable(example).insert(example);
		List<LinkCarCompanyAndLogo> foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(1));

		final LinkCarCompanyAndLogo got = foundLinks.get(0);
		got.fkCarCompany.setValue(2);
		DBActionList updateActions = database.update(got);
		Assert.assertThat(updateActions.size(), is(1));
		Assert.assertThat(updateActions.get(0), instanceOf(DBUpdateSimpleTypesUsingAllColumns.class));

		foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(0));

		DBActionList revertActionList = updateActions.getRevertActionList();
		Assert.assertThat(revertActionList.size(), is(1));
		Assert.assertThat(revertActionList.get(0), instanceOf(DBUpdateToPreviousValues.class));

		revertActionList.execute(database);
		foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(1));
		database.delete(example);
	}
}
