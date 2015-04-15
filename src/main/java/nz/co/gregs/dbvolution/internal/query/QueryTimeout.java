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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Timer;
import nz.co.gregs.dbvolution.databases.DBStatement;

/**
 *
 * @author gregory.graham
 */
public class QueryTimeout extends Timer{

	CancelTask task;

	private QueryTimeout(CancelTask task) {
		this.task = task;
	}

	public static QueryTimeout scheduleTimeout(DBStatement statement, Integer timeoutInMilliseconds) {
		QueryTimeout queryTimeout;
		queryTimeout = new QueryTimeout(new CancelTask(statement));
		Calendar cal = new GregorianCalendar();
		cal.add(Calendar.MILLISECOND, timeoutInMilliseconds);
		System.out.println("SCHEDULING TIMEOUT: " + DateFormat.getDateTimeInstance().format(cal.getTime()));
		queryTimeout.schedule(queryTimeout.task, cal.getTime());
		return queryTimeout;
	}

}
