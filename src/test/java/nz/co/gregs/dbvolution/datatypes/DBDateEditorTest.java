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

import java.util.Date;
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
public class DBDateEditorTest {

	public DBDateEditorTest() {
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
		DBDateEditor instance = new DBDateEditor();
		instance.setFormat(format);
		// TODO review the generated test code and remove the default call to fail.
	}

	/**
	 * Test of setAsText method, of class DBDateEditor.
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testSetAsText() {
		String text = "Sat, 12 Aug 1995 13:30:00 GMT";
		DBDateEditor instance = new DBDateEditor();
		instance.setAsText(text);
		Date dateValue = new Date();
		dateValue.setTime(Date.parse(text));
		Assert.assertThat((Date) ((QueryableDatatype) instance.getValue()).getLiteralValue(), is(dateValue));
	}

}
