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

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnableToInstantiateDBReportSubclassException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * DBvolution needed to create an instance of your DBReport but was unable to
	 * do so.
	 *
	 * <p>
	 * Please ensure all DBReports have a public, argument-less, default
	 * constructor.
	 *
	 * @param badReport badReport
	 * @param ex ex
	 */
	public UnableToInstantiateDBReportSubclassException(Object badReport, Exception ex) {
		super("Unable To Create DBReport Instance: please ensure that your DBReport subclass, " + badReport.getClass().getSimpleName() + ", has a Public, No Parameter Constructor. The class itself may need to be \"public static\" as well.", ex);
	}
}
