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
package nz.co.gregs.dbvolution.actions;

import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import org.junit.Assert;

/**
 *
 * @author gregorygraham
 */
public class DBInsertTest extends AbstractTest {

	public DBInsertTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of save method, of class DBInsert.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testSave() throws Exception {
		System.out.println("save");
		DBRow row = new CarCompany("TATA", 123);
		DBActionList result = DBInsert.save(database, row);
		Assert.assertThat(result.size(), is(1));
		DBAction act = result.get(0);
		if (act instanceof DBInsert) {
			DBInsert insert = (DBInsert) act;
			final List<Long> generatedKeys = insert.getGeneratedPrimaryKeys();
			if (generatedKeys.size() > 0) {
				System.out.println("Primary Key Returned: " + generatedKeys.get(0));
			}
		}
	}
}
