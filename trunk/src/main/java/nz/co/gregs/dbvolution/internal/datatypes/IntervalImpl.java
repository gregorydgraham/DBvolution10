/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.datatypes;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.joda.time.Period;

/**
 *
 * @author gregorygraham
 */
public class IntervalImpl {
	
	private static final String ZERO_INTERVAL_STRING = "P0Y0M0D0h0m0s000";

	public IntervalImpl() {
	}

	public static String getZeroIntervalString() {
		return ZERO_INTERVAL_STRING;
	}

	@SuppressWarnings("deprecation")
	public static String getIntervalString(Date original, Date compareTo) {
		int years = original.getYear() - compareTo.getYear();
		int months = original.getMonth() - compareTo.getMonth();
		int days = original.getDay() - compareTo.getDay();
		int hours = original.getHours() - compareTo.getHours();
		int minutes = original.getMinutes() - compareTo.getMinutes();
		int seconds = original.getSeconds() - compareTo.getSeconds();
		int millis = (int) ((original.getTime() - ((original.getTime() / 1000) * 1000)) - (compareTo.getTime() - ((compareTo.getTime() / 1000) * 1000)));
		String intervalString = "P" + years + "Y" + months + "M" + days + "D" + hours + "h" + minutes + "m" + seconds + "s" + millis;
		return intervalString;
	}

	public static String getIntervalString(Period interval) {
		int years = interval.getYears();
		int months = interval.getMonths();
		int days = interval.getDays() + interval.getWeeks() * 7;
		int hours = interval.getHours();
		int minutes = interval.getMinutes();
		int seconds = interval.getSeconds();
		int millis = interval.getMillis();
		String intervalString = "P" + years + "Y" + months + "M" + days + "D" + hours + "h" + minutes + "m" + seconds + "s" + millis;
		return intervalString;
	}

	public static int compareIntervalStrings(String original, String compareTo) {
		String[] splitOriginal = original.split("[A-Za-z]");
		String[] splitCompareTo = compareTo.split("[A-Za-z]");
		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty
			System.out.println("SPLITORIGINAL "+i+": "+splitOriginal[i]);
			int intOriginal = Integer.parseInt(splitOriginal[i]);
			int intCompareTo = Integer.parseInt(splitCompareTo[i]);
			if (intOriginal > intCompareTo) {
				return 1;
			}
			if (intOriginal < intCompareTo) {
				return -1;
			}
		}
		return 0;
	}

	public static Date addDateAndIntervalString(Date original, String intervalStr) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(original);
		int years = Integer.parseInt(intervalStr.replaceAll(".*P([-0-9.]+)Y.*", "$1"));
		int months = Integer.parseInt(intervalStr.replaceAll(".*Y([-0-9.]+)M.*", "$1"));
		int days = Integer.parseInt(intervalStr.replaceAll(".*M([-0-9.]+)D.*", "$1"));
		int hours = Integer.parseInt(intervalStr.replaceAll(".*D([-0-9.]+)h.*", "$1"));
		int minutes = Integer.parseInt(intervalStr.replaceAll(".*h([-0-9.]+)m.*", "$1"));
		int seconds = Integer.parseInt(intervalStr.replaceAll(".*m([-0-9.]+)s.*", "$1"));
		int millis = Integer.parseInt(intervalStr.replaceAll(".*s([-0-9.]+)$", "$1"));

		cal.add(Calendar.YEAR, years);
		cal.add(Calendar.MONTH, months);
		cal.add(Calendar.DAY_OF_MONTH, days);
		cal.add(Calendar.HOUR, hours);
		cal.add(Calendar.MINUTE, minutes);
		cal.add(Calendar.SECOND, seconds);
		cal.add(Calendar.MILLISECOND, millis);
		return cal.getTime();
	}

	public static Date subtractDateAndIntervalString(Date original, String intervalStr) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(original);
		int years = Integer.parseInt(intervalStr.replaceAll(".*P([-0-9.]+)Y.*", "$1"));
		int months = Integer.parseInt(intervalStr.replaceAll(".*Y([-0-9.]+)M.*", "$1"));
		int days = Integer.parseInt(intervalStr.replaceAll(".*M([-0-9.]+)D.*", "$1"));
		int hours = Integer.parseInt(intervalStr.replaceAll(".*D([-0-9.]+)h.*", "$1"));
		int minutes = Integer.parseInt(intervalStr.replaceAll(".*h([-0-9.]+)m.*", "$1"));
		int seconds = Integer.parseInt(intervalStr.replaceAll(".*m([-0-9.]+)s.*", "$1"));
		int millis = Integer.parseInt(intervalStr.replaceAll(".*s([-0-9.]+)$", "$1"));

		cal.add(Calendar.YEAR, -1 * years);
		cal.add(Calendar.MONTH, -1 * months);
		cal.add(Calendar.DAY_OF_MONTH, -1 * days);
		cal.add(Calendar.HOUR, -1 * hours);
		cal.add(Calendar.MINUTE, -1 * minutes);
		cal.add(Calendar.SECOND, -1 * seconds);
		cal.add(Calendar.MILLISECOND, -1 * millis);
		return cal.getTime();
	}

	public static Period parseIntervalFromGetString(String intervalStr) {
		System.out.println("DBV INTERVAL: "+intervalStr);
		Period interval = new Period();
		interval = interval.withYears(Integer.parseInt(intervalStr.replaceAll(".*P([-0-9.]+)Y.*", "$1")));
		interval = interval.withMonths(Integer.parseInt(intervalStr.replaceAll(".*Y([-0-9.]+)M.*", "$1")));
		interval = interval.withDays(Integer.parseInt(intervalStr.replaceAll(".*M([-0-9.]+)D.*", "$1")));
		interval = interval.withHours(Integer.parseInt(intervalStr.replaceAll(".*D([-0-9.]+)h.*", "$1")));
		interval = interval.withMinutes(Integer.parseInt(intervalStr.replaceAll(".*h([-0-9.]+)m.*", "$1")));
		interval = interval.withSeconds(Integer.parseInt(intervalStr.replaceAll(".*m([-0-9.]+)s.*", "$1")));
		interval = interval.withMillis(Integer.parseInt(intervalStr.replaceAll(".*s([-0-9.]+)$", "$1")));
		return interval;
	}
}
