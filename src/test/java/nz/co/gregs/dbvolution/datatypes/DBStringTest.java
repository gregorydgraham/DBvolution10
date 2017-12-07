/*
 * Copyright 2015 gregorygraham.
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

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBStringTest extends AbstractTest {

	public DBStringTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetValueHandlesUnicode() throws SQLException {
		Marque marque = new Marque();
		marque.name.permittedValuesIgnoreCase("TOYOTA");
		List<Marque> got = database.get(marque);

		got.get(0).isUsedForTAFROs.setValue("Marahāo, Taupō, Años, Grégory");
		database.update(got);

		marque = new Marque();
		marque.isUsedForTAFROs.permittedValuesIgnoreCase("Marahāo, Taupō, Años, Grégory");
		got = database.get(marque);
		Assert.assertThat(got.size(), is(1));
		Assert.assertThat(got.get(0).isUsedForTAFROs.stringValue(), is("Marahāo, Taupō, Años, Grégory"));

	}

	@Test
	public void testGetValueHandlesChinese() throws SQLException {
		Marque marque = new Marque();
		marque.name.permittedValuesIgnoreCase("Toyota");
		List<Marque> got = database.get(marque);
		got.get(0).isUsedForTAFROs.setValue("数据库应该很容易");
		database.update(got);

		marque = new Marque();
		marque.name.permittedValuesIgnoreCase("Toyota");
		got = database.get(marque);
		Assert.assertThat(got.get(0).isUsedForTAFROs.stringValue(), is("数据库应该很容易"));

	}

}
