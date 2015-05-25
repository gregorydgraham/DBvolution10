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

import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalUpdateOfUndefinedRowException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

/**
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
		System.out.println("getUpdates");
		Marque marque = new Marque();
		marque.name.setValue("Not a Marque");
		DBUpdate.getUpdates(marque);
	}

}
