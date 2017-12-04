/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.internal.query;

import java.sql.SQLException;
import java.util.TimerTask;
import nz.co.gregs.dbvolution.databases.DBStatement;

/**
 * A TimerTask for canceling queries.
 * 
 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @author gregory.graham
 */
public class CancelTask extends TimerTask {

	private final DBStatement query;

	/**
	 * Creates a CancelTask for the query.
	 *
	 * @param query
	 */
	public CancelTask(DBStatement query) {
		this.query = query;
	}

	@Override
	public void run() {
		try {
			query.cancel();
		} catch (SQLException ex) {
			if (!ex.getMessage().equals("'Statement' already closed.")&&!ex.getMessage().contains("The object is already closed")) {
				throw new RuntimeException("Exception Occurred During Query Timeout.", ex);
			}
		}
	}

}
