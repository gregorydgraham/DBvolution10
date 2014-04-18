/*
 * Copyright Error: on line 4, column 29 in Templates/Licenses/license-apache20.txt
 Expecting a date here, found: 15/03/2014 gregorygraham.
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

import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregorygraham
 */
public class QueryGraphVisualisationTest {

	public DBDatabase database;

	public static void main(String[] args) throws Exception {
		H2MemoryDB h2MemoryDB = new H2MemoryDB("dbvolutionTest", "", "", false);
		QueryGraphVisualisationTest myObject = new QueryGraphVisualisationTest();
		myObject.setup(h2MemoryDB);
		final LinkCarCompanyAndLogoWithPreviousLink linkCarCompanyAndLogoWithPreviousLink = new LinkCarCompanyAndLogoWithPreviousLink();

		DBQuery dbQuery = h2MemoryDB.getDBQuery(new CarCompany(), new Marque());
		dbQuery.addOptional(new CompanyLogo(), linkCarCompanyAndLogoWithPreviousLink);
		dbQuery.displayQueryGraph();

		tearDown(h2MemoryDB);
	}

	public void setup(DBDatabase database) throws Exception {
		database.setPrintSQLBeforeExecuting(false);
		database.dropTableNoExceptions(new Marque());
		database.createTable(new Marque());

		database.dropTableNoExceptions(new CarCompany());
		database.createTable(new CarCompany());

		database.dropTableNoExceptions(new CompanyLogo());
		database.createTable(new CompanyLogo());

		database.dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.createTable(new LinkCarCompanyAndLogo());

		database.dropTableNoExceptions(new LinkCarCompanyAndLogoWithPreviousLink());
		database.createTable(new LinkCarCompanyAndLogoWithPreviousLink());

		database.setPrintSQLBeforeExecuting(true);
	}

	private static void tearDown(H2MemoryDB database) {
		database.dropTableNoExceptions(new LinkCarCompanyAndLogoWithPreviousLink());
		database.dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.dropTableNoExceptions(new CompanyLogo());
		database.dropTableNoExceptions(new CarCompany());
		database.dropTableNoExceptions(new Marque());
	}

}
