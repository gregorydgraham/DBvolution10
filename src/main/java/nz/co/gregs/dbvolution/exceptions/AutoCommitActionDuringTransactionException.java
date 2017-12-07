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
public class AutoCommitActionDuringTransactionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when a Data Definition Language (DDL) operation is used in a
	 * transaction.
	 *
	 * <p>
	 * DDL operations force a commit and are virtually always a mistake inside a
	 * transaction.
	 *
	 * @param ddlMethod	ddlMethod
	 */
	public AutoCommitActionDuringTransactionException(String ddlMethod) {
		super("Autocommit Action Attempted During Transaction: the method " + ddlMethod + " will cause a commit during your read-only transaction, this is probably not what you want. Remove the call from the transaction.");
	}

}
