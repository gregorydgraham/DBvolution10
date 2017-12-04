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

import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class RecognisorTest {

	@Test
	public void testPkRecognisor() {
		PrimaryKeyRecognisor pkRecognisor = new PrimaryKeyRecognisor();
		Assert.assertFalse(pkRecognisor.isPrimaryKeyColumn("table", "column"));

		pkRecognisor = new UIDBasedPKRecognisor();
		Assert.assertFalse(pkRecognisor.isPrimaryKeyColumn("table", "column"));
		Assert.assertTrue(pkRecognisor.isPrimaryKeyColumn("table", "uid_table"));
	}

	@Test
	public void testFKRecognisor() {
		ForeignKeyRecognisor fkRecognisor = new ForeignKeyRecognisor();
		Assert.assertFalse(fkRecognisor.isForeignKeyColumn("table", "column"));
		Assert.assertNull(fkRecognisor.getReferencedColumn("table", "column"));
		Assert.assertNull(fkRecognisor.getReferencedTable("table", "column"));

		fkRecognisor = new FKBasedFKRecognisor();
		Assert.assertFalse(fkRecognisor.isForeignKeyColumn("table", "column"));
		Assert.assertNull(fkRecognisor.getReferencedColumn("table", "column"));
		Assert.assertNull(fkRecognisor.getReferencedTable("table", "column"));

		Assert.assertTrue(fkRecognisor.isForeignKeyColumn("table", "fk_table"));
		Assert.assertEquals("uid_table", fkRecognisor.getReferencedColumn("table", "fk_table"));
		Assert.assertEquals("Table", fkRecognisor.getReferencedTable("table", "fk_table"));

		Assert.assertTrue(fkRecognisor.isForeignKeyColumn("t_4", "fk_3"));
		Assert.assertEquals("uid_3", fkRecognisor.getReferencedColumn("t_4", "fk_3"));
		Assert.assertEquals("T_3", fkRecognisor.getReferencedTable("t_4", "fk_3"));

		Assert.assertTrue(fkRecognisor.isForeignKeyColumn("t_4", "fk_3_1"));
		Assert.assertEquals("uid_3", fkRecognisor.getReferencedColumn("t_4", "fk_3_1"));
		Assert.assertEquals("T_3", fkRecognisor.getReferencedTable("t_4", "fk_3_1"));

	}

}
