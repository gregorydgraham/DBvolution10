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

import java.util.Set;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class RelatedTableTest extends AbstractTest {

	public RelatedTableTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void getReferencedTablesTest() {
		Marque marque = new Marque();

		Set<Class<? extends DBRow>> allReferencedTables = (new CarCompany()).getReferencedTables();
		assertThat(allReferencedTables.size(), is(0));

		allReferencedTables = marque.getReferencedTables();
		assertThat(allReferencedTables.size(), is(1));
		assertThat(allReferencedTables.contains(CarCompany.class), is(true));

		allReferencedTables = (new LinkCarCompanyAndLogo()).getReferencedTables();
		assertThat(allReferencedTables.size(), is(2));
		assertThat(allReferencedTables.contains(CarCompany.class), is(true));
		assertThat(allReferencedTables.contains(CompanyLogo.class), is(true));
	}

	@Test
	public void getAllConnectedTablesTest() {
		Marque marque = new Marque();

		Set<Class<? extends DBRow>> allConnectedTables = (new CarCompany()).getAllConnectedTables();
		assertThat(allConnectedTables.size(), is(6));
		assertThat(allConnectedTables.contains(Marque.class), is(true));
		assertThat(allConnectedTables.contains(MarqueSelectQuery.class), is(true));
		assertThat(allConnectedTables.contains(CompanyLogo.class), is(true));
		assertThat(allConnectedTables.contains(CompanyText.class), is(true));
		assertThat(allConnectedTables.contains(LinkCarCompanyAndLogo.class), is(true));

		allConnectedTables = marque.getAllConnectedTables();
		assertThat(allConnectedTables.size(), is(1));
		assertThat(allConnectedTables.contains(CarCompany.class), is(true));

		allConnectedTables = (new LinkCarCompanyAndLogo()).getAllConnectedTables();
		assertThat(allConnectedTables.size(), is(2));
		assertThat(allConnectedTables.contains(CarCompany.class), is(true));
		assertThat(allConnectedTables.contains(CompanyLogo.class), is(true));
	}

	@Test
	public void getRelatedTablesTest() {
		Marque marque = new Marque();

		Set<Class<? extends DBRow>> allRelatedTables = (new CarCompany()).getRelatedTables();
		assertThat(allRelatedTables.size(), is(6));
		assertThat(allRelatedTables.contains(Marque.class), is(true));
		assertThat(allRelatedTables.contains(MarqueSelectQuery.class), is(true));
		assertThat(allRelatedTables.contains(CompanyLogo.class), is(true));
		assertThat(allRelatedTables.contains(CompanyText.class), is(true));
		assertThat(allRelatedTables.contains(LinkCarCompanyAndLogo.class), is(true));
		assertThat(allRelatedTables.contains(LinkCarCompanyAndLogoWithPreviousLink.class), is(true));

		allRelatedTables = marque.getRelatedTables();
		assertThat(allRelatedTables.size(), is(0));

		allRelatedTables = (new LinkCarCompanyAndLogo()).getRelatedTables();
		assertThat(allRelatedTables.size(), is(0));
	}
}
