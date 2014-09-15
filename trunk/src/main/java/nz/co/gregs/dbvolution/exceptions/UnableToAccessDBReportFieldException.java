/*
 * Copyright 2014 gregorygraham.
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
 *
 * @author Gregory Graham
 */
public class UnableToAccessDBReportFieldException extends RuntimeException {

		public static final long serialVersionUID = 1L;

		public UnableToAccessDBReportFieldException(Object badReport, Field field, Exception ex) {
			super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null: Especially field: " + field.getName(), ex);
		}

		public UnableToAccessDBReportFieldException(Object badReport, Exception ex) {
			super("Unable To Access DBReport Field: please ensure that all DBReport fields on " + badReport.getClass().getSimpleName() + " are Public and Non-Null.", ex);
		}
	}