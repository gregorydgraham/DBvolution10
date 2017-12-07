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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Uses the default formatting provided in DBDate or the
 * {@link SimpleDateFormat} format provided.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDateEditor extends PropertyEditorSupport {

	private String format = null;

	/**
	 *
	 * @param format format
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	/**
	 *
	 * @param text text
	 */
	@Override
	public void setAsText(String text) {
		if (format != null) {
			try {
				Date parse = new SimpleDateFormat(format).parse(text);
				DBDate type = new DBDate(parse);
				setValue(type);
			} catch (ParseException ex) {
				DBDate type = new DBDate(text);
				setValue(type);
			}
		} else {
			DBDate type = new DBDate(text);
			setValue(type);
		}
	}
}
