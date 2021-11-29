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
package nz.co.gregs.dbvolution.transactions;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;

/**
 *
 * @author Gregory Graham
 * @param <V> The return type of the transaction
 *
 */
public interface DBTransaction<V> {

	/**
	 * Perform the transaction on the database, returning TRUE if the transaction
	 * succeeded, or FALSE if it did not.
	 *
	 * @param dbDatabase	the database the transaction is to be performed on.
	 * @return TRUE if the transaction completed without errors, FALSE otherwise.
	 * @throws ExceptionThrownDuringTransaction if the transaction throws an exception
	 *
	 */
	V doTransaction(DBDatabase dbDatabase) throws ExceptionThrownDuringTransaction;
}
