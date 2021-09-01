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
package nz.co.gregs.dbvolution.generation;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 *
 * @author Gregory Graham
 */
public class RecognisorTest {

	@Test
	public void testPkRecognisor() {
		PrimaryKeyRecognisor pkRecognisor = new PrimaryKeyRecognisor();
		assertFalse(pkRecognisor.isPrimaryKeyColumn("table", "column"));

		pkRecognisor = new UIDBasedPKRecognisor();
		assertFalse(pkRecognisor.isPrimaryKeyColumn("table", "column"));
		assertTrue(pkRecognisor.isPrimaryKeyColumn("table", "uid_table"));
	}

	@Test
	public void testFKRecognisor() {
		ForeignKeyRecognisor fkRecognisor = new ForeignKeyRecognisor();
		assertFalse(fkRecognisor.isForeignKeyColumn("table", "column"));
		assertNull(fkRecognisor.getReferencedColumn("table", "column"));
		assertNull(fkRecognisor.getReferencedTable("table", "column"));

		fkRecognisor = new FKBasedFKRecognisor();
		assertFalse(fkRecognisor.isForeignKeyColumn("table", "column"));
		assertNull(fkRecognisor.getReferencedColumn("table", "column"));
		assertNull(fkRecognisor.getReferencedTable("table", "column"));

		assertTrue(fkRecognisor.isForeignKeyColumn("table", "fk_table"));
		assertEquals("uid_table", fkRecognisor.getReferencedColumn("table", "fk_table"));
		assertEquals("Table", fkRecognisor.getReferencedTable("table", "fk_table"));

		assertTrue(fkRecognisor.isForeignKeyColumn("t_4", "fk_3"));
		assertEquals("uid_3", fkRecognisor.getReferencedColumn("t_4", "fk_3"));
		assertEquals("T_3", fkRecognisor.getReferencedTable("t_4", "fk_3"));

		assertTrue(fkRecognisor.isForeignKeyColumn("t_4", "fk_3_1"));
		assertEquals("uid_3", fkRecognisor.getReferencedColumn("t_4", "fk_3_1"));
		assertEquals("T_3", fkRecognisor.getReferencedTable("t_4", "fk_3_1"));

	}

}
