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
package nz.co.gregs.dbvolution.datatypes;

import java.beans.PropertyEditorSupport;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class QueryableDatatypeEditor extends PropertyEditorSupport {

	private String format;

	/**
	 * Only "upperCase" is supported.
	 *
	 * @param format	format
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public void setAsText(String text) {
		String inputText = text;
		if (format != null && format.equals("upperCase")) {
			inputText = inputText.toUpperCase();
		}
		Object value = getValue();
		if (value instanceof QueryableDatatype) {
			QueryableDatatype<?> qdt = (QueryableDatatype<?>) value;
			qdt.setValueFromStandardStringEncoding(inputText);
			setValue(qdt);
		} else {
			DBString type = new DBString();
			type.setValueFromStandardStringEncoding(inputText);
			setValue(type);
		}
	}
}
