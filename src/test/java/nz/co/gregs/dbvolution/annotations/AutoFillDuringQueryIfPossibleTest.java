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
package nz.co.gregs.dbvolution.annotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class AutoFillDuringQueryIfPossibleTest extends AbstractTest {

	public AutoFillDuringQueryIfPossibleTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testFillingSimpleField() throws SQLException {
		DBQuery query = database.getDBQuery(new FilledMarque(), new CarCompany()).setBlankQueryAllowed(true);
		query.getAllRows();
		List<FilledMarque> instances = query.getAllInstancesOf(new FilledMarque());

		for (FilledMarque instance : instances) {
			final CarCompany relatedCarCo = query.getQueryDetails().getRelatedInstancesFromQuery(instance, new CarCompany()).get(0);
			final CarCompany actualCarCo = instance.actualCarCo;
			if (actualCarCo == null) {
				Assert.assertThat(relatedCarCo, nullValue());
			} else {
				Assert.assertThat(relatedCarCo.name.stringValue(), is(actualCarCo.name.stringValue()));
			}
		}
	}

	@Test
	public void testFillingArray() throws SQLException, Exception {
		DBQuery query = database.getDBQuery(new FilledCarCoWithArray(), new Marque()).setBlankQueryAllowed(true);
		query.getAllRows();
		List<FilledCarCoWithArray> instances = query.getAllInstancesOf(new FilledCarCoWithArray());

		for (FilledCarCoWithArray instance : instances) {
			if (instance.marques != null) {
				final List<Marque> relateds = query.getQueryDetails().getRelatedInstancesFromQuery(instance, new Marque());
				final Marque[] actuals = instance.marques;
				Assert.assertThat(relateds, contains(actuals));
			} else {
				throw new Exception("Marque should have been found for " + instance.name);
			}
		}
	}

	@Test
	public void testFillingList() throws SQLException, Exception {
		final FilledCarCoWithList testExample = new FilledCarCoWithList();
		DBQuery query = database.getDBQuery(testExample, new Marque()).setBlankQueryAllowed(true);
		query.getAllRows();
		List<FilledCarCoWithList> instances = query.getAllInstancesOf(testExample);
		for (FilledCarCoWithList instance : instances) {
			if (instance.marques != null) {
				final List<Marque> relateds = query.getQueryDetails().getRelatedInstancesFromQuery(instance, new Marque());
				List<String> relatedNames = new ArrayList<String>();
				List<String> actualNames = new ArrayList<String>();
				for (Marque related : relateds) {
					relatedNames.add(related.name.stringValue());
				}
				final List<Marque> actuals = instance.marques;
				for (Marque actual : actuals) {
					actualNames.add(actual.name.stringValue());
				}
				Assert.assertThat(relatedNames, contains(actualNames.toArray(new String[]{})));
			} else {
				throw new Exception("Marque should have been found for " + instance.name);
			}
		}
	}

	public static class FilledMarque extends Marque {

		private static final long serialVersionUID = 1L;

		@AutoFillDuringQueryIfPossible
		public CarCompany actualCarCo;
	}

	public static class FilledCarCoWithArray extends CarCompany {

		private static final long serialVersionUID = 1L;

		@AutoFillDuringQueryIfPossible
		public Marque[] marques;
	}

	public static class FilledCarCoWithList extends CarCompany {

		private static final long serialVersionUID = 1L;

		@AutoFillDuringQueryIfPossible(requiredClass = Marque.class)
		public List<Marque> marques;
	}

}
