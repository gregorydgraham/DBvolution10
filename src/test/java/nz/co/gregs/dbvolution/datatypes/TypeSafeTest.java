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
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

public class TypeSafeTest extends AbstractTest {

	public TypeSafeTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void typeSafeTest() {
		DBInteger myInt = new DBInteger(23);
		if (!myInt.getValue().equals(23L)) {
			throw new RuntimeException("DBInteger(int) should return a Long");
		}
		myInt.setValue(23);
		if (!myInt.getValue().equals(23L)) {
			throw new RuntimeException("DBInteger.setValue(int) should return a Long");
		}
		myInt = new DBInteger(23L);
		if (!myInt.getValue().equals(23L)) {
			throw new RuntimeException("DBInteger.setValue(long) should return a Long");
		}
		myInt.setValue(23L);
		if (!myInt.getValue().equals(23L)) {
			throw new RuntimeException("DBInteger.setValue(long) should return a Long");
		}
	}

}
