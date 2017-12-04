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
package nz.co.gregs.dbvolution.exceptions;

import java.lang.reflect.Field;

/**
 * Thrown when DBvolution is unable to access a field it needs.
 *
 * <p>
 * A lot of reflection is used in DBV, please ensure that the fields are
 * publicly accessible and non-null.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnableToAccessDBReportFieldException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when DBvolution is unable to access a field it needs.
	 *
	 * <p>
	 * A lot of reflection is used in DBV, please ensure that the fields are
	 * publicly accessible and non-null.
	 *
	 * @param badReport badReport
	 * @param field field
	 * @param ex ex
	 */
	public UnableToAccessDBReportFieldException(Object badReport, Field field, Exception ex) {
		super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null: Especially field: " + field.getName(), ex);
	}

	/**
	 * Thrown when DBvolution is unable to access a field it needs.
	 *
	 * <p>
	 * A lot of reflection is used in DBV, please ensure that the fields are
	 * publicly accessible and non-null.
	 *
	 * @param badReport badReport
	 * @param ex ex
	 */
	public UnableToAccessDBReportFieldException(Object badReport, Exception ex) {
		super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null.", ex);
	}
}
