/*
 * Copyright 2013 Gregory Graham.
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

import java.sql.SQLException;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class CopyDBRowTest extends AbstractTest {

	public CopyDBRowTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void copyDBRowTest() throws SQLException {
		DBTable<Marque> marqs = database.getDBTable(new Marque());
		marqs.setBlankQueryAllowed(true);
		Marque first = marqs.getAllRows().get(0);
		Marque firstCopy = DBRow.copyDBRow(first);

		Assert.assertThat(firstCopy, is(not(first)));
		Assert.assertThat(firstCopy.name, not(sameInstance(first.name)));
		Assert.assertThat(firstCopy.name, is(first.name));
		Assert.assertThat(firstCopy.carCompany.getValue(), is(equalTo(first.carCompany.getValue())));
		Assert.assertThat(firstCopy.name.getValue(), is(equalTo(first.name.getValue())));
		Assert.assertThat(firstCopy.creationDate.getValue(), is(equalTo(first.creationDate.getValue())));
	}

}
