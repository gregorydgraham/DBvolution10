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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Timer;
import nz.co.gregs.dbvolution.databases.DBStatement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class QueryTimeout extends Timer {

	CancelTask task;

	private QueryTimeout(CancelTask task) {
		this.task = task;
	}

	/**
	 * Creates the timer and thread that stops this query from overrunning the
	 * timeout.
	 *
	 * @param statement
	 * @param timeoutInMilliseconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this timer
	 */
	public static QueryTimeout scheduleTimeout(DBStatement statement, Integer timeoutInMilliseconds) {
		QueryTimeout queryTimeout;
		queryTimeout = new QueryTimeout(new CancelTask(statement));
		Calendar cal = new GregorianCalendar();
		cal.add(Calendar.MILLISECOND, timeoutInMilliseconds);
		queryTimeout.schedule(queryTimeout.task, cal.getTime());
		return queryTimeout;
	}

}
