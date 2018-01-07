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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Thrown when the current object needs the supplied variable to be a field of
 * the object, but it isn't.
 *
 * <p>
 * A common pattern in DBvolution is "row.method(row.field)" allowing the object
 * to find the field within itself and look up the annotations associated with
 * the field.
 *
 * <p>
 * However if the variable supplied, "row.field" above, is not actually a field
 * of the object, then it cannot resolve the field and cannot find the required
 * annotations. This would happen if you did "row.method(new Object())", or
 * "row.method(otherRow.field)".
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class IncorrectRowProviderInstanceSuppliedException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the current object needs the supplied variable to be a field of
	 * the object, but it isn't.
	 */
	public IncorrectRowProviderInstanceSuppliedException() {
		super("The Field Supplied Is Not A Field Of This Instance: use only fields from this instance.");
	}

	/**
	 * Thrown when the current object needs the supplied variable to be a field of
	 * the object, but it isn't.
	 *
	 * @param row row
	 * @param qdt qdt
	 */
	public IncorrectRowProviderInstanceSuppliedException(RowDefinition row, Object qdt) {
		super(constructMessage(row, qdt));
	}

	/**
	 * Thrown when the current object needs the supplied variable to be a field of
	 * the object, but it isn't.
	 *
	 * @param qdt	qdt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an IncorrectRowProviderInstanceSuppliedException
	 */
	public static IncorrectRowProviderInstanceSuppliedException newMultiRowInstance(Object qdt) {
		StringBuilder buf = new StringBuilder();
		buf.append("The ");
		if (qdt == null) {
			buf.append("null");
		} else {
			buf.append(qdt.getClass().getSimpleName());
		}
		buf.append(" Field Supplied Is Not A Field Of Any Of The DBRow ");
		buf.append(" Instances: use only fields from these instances.");
		return new IncorrectRowProviderInstanceSuppliedException(buf.toString());
	}

	/**
	 * Thrown when the current object needs the supplied variable to be a field of
	 * the object, but it isn't.
	 *
	 * @param message	message
	 */
	public IncorrectRowProviderInstanceSuppliedException(String message) {
		super(message);
	}

	private static String constructMessage(RowDefinition row, Object qdt) {
		StringBuilder buf = new StringBuilder();
		buf.append("The ");
		if (qdt == null) {
			buf.append("null");
		} else {
			buf.append(qdt.getClass().getSimpleName());
		}
		buf.append(" Field Supplied Is Not A Field Of The ");
		if (row == null) {
			buf.append("null");
		} else {
			buf.append(row.getClass().getSimpleName());
		}
		buf.append(" Instance: use only fields from this instance.");
		return buf.toString();
	}
}
