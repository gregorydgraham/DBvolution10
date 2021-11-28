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
import java.util.Optional;
import nz.co.gregs.regexi.Regex;
import nz.co.gregs.regexi.RegexReplacement;
import nz.co.gregs.regexi.RegexSplitter;
import nz.co.gregs.regexi.RegexValueFinder;
import org.joda.time.Period;

/**
 *
 * @author gregorygraham
 */
public class DateRepeatImpl {

	private static final String ZERO_DATEREPEAT_STRING = "P0Y0M0D0h0n0.0s";

	/**
	 * Default constructor
	 *
	 */
	public DateRepeatImpl() {
	}

	/**
	 *
	 *
	 * @return the DateRepeat version of Zero
	 */
	public static String getZeroDateRepeatString() {
		return ZERO_DATEREPEAT_STRING;
	}

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return the DateRepeat the represents the difference between these 2 dates
	 */
	@SuppressWarnings("deprecation")
	public static String repeatFromTwoDates(Date original, Date compareTo) {
		if (original == null || compareTo == null) {
			return null;
		}
		Calendar origCal = GregorianCalendar.getInstance();
		origCal.setTime(original);
		Calendar compCal = GregorianCalendar.getInstance();
		compCal.setTime(compareTo);

		int years = origCal.get(Calendar.YEAR) - compCal.get(Calendar.YEAR);
		int months = origCal.get(Calendar.MONTH) - compCal.get(Calendar.MONTH);
		int days = origCal.get(Calendar.DAY_OF_MONTH) - compCal.get(Calendar.DAY_OF_MONTH);
		int hours = origCal.get(Calendar.HOUR_OF_DAY) - compCal.get(Calendar.HOUR_OF_DAY);
		int minutes = origCal.get(Calendar.MINUTE) - compCal.get(Calendar.MINUTE);
		int seconds = origCal.get(Calendar.SECOND) - compCal.get(Calendar.SECOND);
		int millis = origCal.get(Calendar.MILLISECOND) - compCal.get(Calendar.MILLISECOND);

		String intervalString = "P" + years + "Y" + months + "M" + days + "D" + hours + "h" + minutes + "n" + seconds + "." + millis + "s";
		return intervalString;
	}

	/**
	 *
	 * @param interval
	 * @return the DateRepeat equivalent of the Period value
	 */
	public static String getDateRepeatString(Period interval) {
		if (interval == null) {
			return null;
		}
		int years = interval.getYears();
		int months = interval.getMonths();
		int days = interval.getDays() + interval.getWeeks() * 7;
		int hours = interval.getHours();
		int minutes = interval.getMinutes();

		int millis = interval.getMillis();
		double seconds = interval.getSeconds() + (millis / 1000.0);
		String intervalString = "P" + years + "Y" + months + "M" + days + "D" + hours + "h" + minutes + "n" + seconds + "s";
		return intervalString;
	}

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return TRUE if the DateRepeats are the same, otherwise FALSE
	 */
	public static boolean isEqualTo(String original, String compareTo) {
		return compareDateRepeatStrings(original, compareTo) == 0;
	}

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return TRUE if the first DateRepeat value is greater than the second,
	 * otherwise FALSE
	 */
	public static boolean isGreaterThan(String original, String compareTo) {
		return compareDateRepeatStrings(original, compareTo) == 1;
	}

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return TRUE if the first DateRepeat value is less than the second,
	 * otherwise FALSE
	 */
	public static boolean isLessThan(String original, String compareTo) {
		return compareDateRepeatStrings(original, compareTo) == -1;
	}

	static final private RegexSplitter DATE_REPEAT_SPLITTER = Regex.empty().beginSetIncluding().includeLetters().endSet().toSplitter();

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return -1 if the first DateRepeat is the smallest, 0 if they are equal,
	 * and 1 if the first is the largest.
	 */
	public static Integer compareDateRepeatStrings(String original, String compareTo) {
		if (original == null || compareTo == null) {
			return null;
		}
		String[] splitOriginal = DATE_REPEAT_SPLITTER.split(original);
		String[] splitCompareTo = DATE_REPEAT_SPLITTER.split(compareTo);
		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty
			double intOriginal = Double.parseDouble(splitOriginal[i]);
			double intCompareTo = Double.parseDouble(splitCompareTo[i]);
			if (intOriginal > intCompareTo) {
				return 1;
			}
			if (intOriginal < intCompareTo) {
				return -1;
			}
		}
		return 0;
	}

	/**
	 *
	 * @param original the first date.
	 * @param intervalStr the DateRepeat to offset the date.
	 *
	 * @return the Date value offset by the DateRepeat value.
	 */
	public static Date addDateAndDateRepeatString(Date original, String intervalStr) {
		if (original == null || intervalStr == null || intervalStr.length() == 0 || original.toString().length() == 0) {
			return null;
		}
		Calendar cal = new GregorianCalendar();
		cal.setTime(original);
		int years = getYearPart(intervalStr);
		int months = getMonthPart(intervalStr);
		int days = getDayPart(intervalStr);
		int hours = getHourPart(intervalStr);
		int minutes = getMinutePart(intervalStr);
		int seconds = getSecondPart(intervalStr);

		int millis = getMillisecondPart(intervalStr);

		cal.add(Calendar.YEAR, years);
		cal.add(Calendar.MONTH, months);
		cal.add(Calendar.DAY_OF_MONTH, days);
		cal.add(Calendar.HOUR, hours);
		cal.add(Calendar.MINUTE, minutes);
		cal.add(Calendar.SECOND, seconds);
		cal.add(Calendar.MILLISECOND, millis);
		return cal.getTime();
	}

	private static final RegexReplacement NORMALISE_DATEREPEAT = Regex.empty().beginSetExcluding().excludeLiterals(".PYMDhns").excludeRange('0', '9').excludeMinus().endSet().oneOrMore().remove();

	/**
	 *
	 * @param original the first date.
	 * @param intervalInput the DateRepeat to offset the date.
	 *
	 * @return the Date shift backwards (towards the past) by the DateRepeat
	 * value.
	 */
	public static Date subtractDateAndDateRepeatString(Date original, String intervalInput) {
		if (original == null || intervalInput == null || intervalInput.length() == 0) {
			return null;
		}
		String intervalStr = NORMALISE_DATEREPEAT.replaceAll(intervalInput);
		Calendar cal = new GregorianCalendar();
		cal.setTime(original);
		int years = getYearPart(intervalStr);
		int months = getMonthPart(intervalStr);
		int days = getDayPart(intervalStr);
		int hours = getHourPart(intervalStr);
		int minutes = getMinutePart(intervalStr);
		int seconds = getSecondPart(intervalStr);
		int millis = getMillisecondPart(intervalStr);

		cal.add(Calendar.YEAR, -1 * years);
		cal.add(Calendar.MONTH, -1 * months);
		cal.add(Calendar.DAY_OF_MONTH, -1 * days);
		cal.add(Calendar.HOUR, -1 * hours);
		cal.add(Calendar.MINUTE, -1 * minutes);
		cal.add(Calendar.SECOND, -1 * seconds);
		cal.add(Calendar.MILLISECOND, -1 * millis);
		return cal.getTime();
	}

	/**
	 *
	 * @param intervalStr the DateRepeat to parse
	 *
	 * @return the DateRepeat value represented by the String value
	 */
	public static Period parseDateRepeatFromGetString(String intervalStr) {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}

		Period interval = new Period();
		interval = interval.withYears(getYearPart(intervalStr));
		interval = interval.withMonths(getMonthPart(intervalStr));
		interval = interval.withDays(getDayPart(intervalStr));
		interval = interval.withHours(getHourPart(intervalStr));
		interval = interval.withMinutes(getMinutePart(intervalStr));
		interval = interval.withSeconds(getSecondPart(intervalStr));
		interval = interval.withMillis(getMillisecondPart(intervalStr));
		return interval;
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the fractional seconds to millisecond precision
	 * @throws NumberFormatException parsing is used to interpret the seconds so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getMillisecondPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		Double secondsDouble = parseValueDouble(FIND_SECOND_VALUE, intervalStr);
		final int secondsInt = secondsDouble.intValue();
		final Double millisDouble = secondsDouble * 1000.0 - secondsInt * 1000;
		final int millis = millisDouble.intValue();
		return millis;
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the integer and fractional seconds part of the DateRepeat
	 * @throws NumberFormatException parsing is used to interpret the seconds so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getSecondPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		Double valueOf = parseValueDouble(FIND_SECOND_VALUE, intervalStr);
		return valueOf.intValue();
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the minutes part of the DateRepeat
	 * @throws NumberFormatException parsing is used to interpret the minutes so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getMinutePart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		return parseValue(FIND_MINUTE_VALUE, intervalStr);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the hour part of the DateRepeat value
	 * @throws NumberFormatException parsing is used to interpret the numbers so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getHourPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		return parseValue(FIND_HOUR_VALUE, intervalStr);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the day part of the DateRepeat value
	 * @throws NumberFormatException parsing is used to interpret the numbers so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getDayPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		return parseValue(FIND_DAY_VALUE, intervalStr);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the month part of the DateRepeat value
	 * @throws NumberFormatException parsing is used to interpret the numbers so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getMonthPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		return parseValue(FIND_MONTH_VALUE, intervalStr);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the year part of the DateRepeat value
	 * @throws NumberFormatException parsing is used to interpret the numbers so a
	 * NumberFormatException maybe thrown if the intervalStr is malformed
	 */
	public static Integer getYearPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		return parseValue(FIND_YEAR_VALUE, intervalStr);
	}

	private static Integer parseValue(RegexValueFinder finder, String intervalStr) throws NumberFormatException {
		Optional<String> value = finder.getValueFrom(intervalStr);
		return value.isPresent() ? Integer.parseInt(value.get()) : null;
	}

	private static Double parseValueDouble(RegexValueFinder finder, String intervalStr) throws NumberFormatException {
		Optional<String> value = finder.getValueFrom(intervalStr);
		return value.isPresent() ? Double.parseDouble(value.get()) : null;
	}

	private static final RegexValueFinder FIND_YEAR_VALUE = Regex.startingAnywhere()
			.literal("P")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("Y").returnValueFor("value");

	private static final RegexValueFinder FIND_MONTH_VALUE = Regex.startingAnywhere()
			.literal("Y")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("M").returnValueFor("value");

	private static final RegexValueFinder FIND_DAY_VALUE = Regex.startingAnywhere()
			.literal("M")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("D").returnValueFor("value");

	private static final RegexValueFinder FIND_HOUR_VALUE = Regex.startingAnywhere()
			.literal("D")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("h").returnValueFor("value");

	private static final RegexValueFinder FIND_MINUTE_VALUE = Regex.startingAnywhere()
			.literal("h")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("n").returnValueFor("value");

	private static final RegexValueFinder FIND_SECOND_VALUE = Regex.startingAnywhere()
			.literal("n")
			.beginNamedCapture("value")
			.numberLike()
			.oneOrMore()
			.endNamedCapture()
			.literal("s").returnValueFor("value");
}
