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
package nz.co.gregs.dbvolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.*;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class DBScriptTest extends AbstractTest {

	public DBScriptTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of implement method, of class DBScript.
	 * @throws java.lang.Exception
	 */
	@Test
	public void testImplement() throws Exception {
		System.out.println("test");		
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.implement(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()+2));
	}

	/**
	 * Test of test method, of class DBScript.
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTest() throws Exception {
		System.out.println("test");
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.test(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()));
	}

	public class ScriptThatAdds2Marques extends DBScript {

		@Override
		public DBActionList script(DBDatabase db) throws Exception {
			DBActionList actions = new DBActionList();
			Marque myTableRow = new Marque();
			DBTable<Marque> marques = DBTable.getInstance(db, myTableRow);
			myTableRow.getUidMarque().setValue(999);
			myTableRow.getName().setValue("TOYOTA");
			myTableRow.getNumericCode().setValue(10);
			actions.addAll(marques.insert(myTableRow));
			marques.setBlankQueryAllowed(true).getAllRows();
			marques.print();

			List<Marque> myTableRows = new ArrayList<Marque>();
			myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));

			actions.addAll(marques.insert(myTableRows));

			marques.getAllRows();
			marques.print();
			return actions;
		}
	}

}
