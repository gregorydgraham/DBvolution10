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
package nz.co.gregs.dbvolution.datatypes;

import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBNumberEditorTest {

	public DBNumberEditorTest() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	/**
	 * Test of setFormat method, of class DBDateEditor.
	 */
	@Test
	public void testSetFormat() {
		String format = "";
		DBNumberEditor instance = new DBNumberEditor();
		instance.setFormat(format);
		// TODO review the generated test code and remove the default call to fail.
	}

	/**
	 * Test of setAsText method, of class DBDateEditor.
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testSetAsText() {
		String text = "5.1";
		DBNumberEditor instance = new DBNumberEditor();
		instance.setAsText(text);
		Assert.assertThat((Double) ((QueryableDatatype) instance.getValue()).getLiteralValue(), is(5.1));
	}

}
