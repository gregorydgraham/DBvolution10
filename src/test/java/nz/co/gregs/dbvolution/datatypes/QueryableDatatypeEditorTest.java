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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class QueryableDatatypeEditorTest {

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
		QueryableDatatypeEditor instance = new QueryableDatatypeEditor();
		instance.setFormat(format);
	}

	/**
	 * Test of setAsText method, of class DBDateEditor.
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testSetAsText() {
		String fivePointOne = "5.1";
		QueryableDatatypeEditor instance = new QueryableDatatypeEditor();
		instance.setAsText(fivePointOne);
		assertThat(((QueryableDatatype) instance.getValue()).stringValue(), is("5.1"));

		instance = new QueryableDatatypeEditor();
		instance.setValue(new DBInteger());
		instance.setAsText(fivePointOne);
		assertThat(((QueryableDatatype) instance.getValue()).stringValue(), is("5"));
	}
}
