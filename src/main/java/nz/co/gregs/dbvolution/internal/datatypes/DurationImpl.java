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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import nz.co.gregs.regexi.Regex;
import nz.co.gregs.regexi.RegexSplitter;
import nz.co.gregs.regexi.RegexValueFinder;

/**
 *
 *
 * @author gregorygraham
 */
public class DurationImpl {

	private static final String ZERO_DURATION_STRING = "P0DT0H0M0.0S";
	private static final String DAYS_CAPTURE = "days";
	private static final String HOURS_CAPTURE = "hours";
	private static final String MINUTES_CAPTURE = "minutes";
	private static final String SECONDS_CAPTURE = "seconds";
	private static final Regex REGEX = Regex.empty()
			.literal("P").beginNamedCapture(DAYS_CAPTURE).numberLike().endNamedCapture()
			.literal("DT").beginNamedCapture(HOURS_CAPTURE).numberLike().endNamedCapture()
			.literal("H").beginNamedCapture(MINUTES_CAPTURE).numberLike().endNamedCapture()
			.literal("M").beginNamedCapture(SECONDS_CAPTURE).numberLike().endNamedCapture()
			.literal("S").toRegex();
	private static final RegexValueFinder GET_DAYS = REGEX.returnValueFrom(DAYS_CAPTURE);
	private static final RegexValueFinder GET_HOURS = REGEX.returnValueFrom(HOURS_CAPTURE);
	private static final RegexValueFinder GET_MINUTES = REGEX.returnValueFrom(MINUTES_CAPTURE);
	private static final RegexValueFinder GET_SECONDS = REGEX.returnValueFrom(SECONDS_CAPTURE);

	/**
	 * Default constructor
	 *
	 */
	public DurationImpl() {
	}

	/**
	 *
	 *
	 * @return the DateRepeat version of Zero
	 */
	public static String getZeroDurationString() {
		return ZERO_DURATION_STRING;
	}

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return the DateRepeat the represents the difference between these 2 dates
	 */
	@SuppressWarnings("deprecation")
	public static String repeatFromTwoDates(LocalDateTime original, LocalDateTime compareTo) {
		if (original == null || compareTo == null) {
			return null;
		}
		final Instant originalInstant = original.toInstant(ZoneOffset.UTC);
		double originalTime = 0.0d + originalInstant.getEpochSecond() + ((0.0d + originalInstant.getNano()) / 1000000000.0d);
		final Instant compareInstant = compareTo.toInstant(ZoneOffset.UTC);
		double compareTime = 0.0d + compareInstant.getEpochSecond() + ((0.0d + compareInstant.getNano()) / 1000000000.0d);
		double differenceInSeconds = originalTime - compareTime;
		int days = (int) (differenceInSeconds / (SECONDS_IN_A_DAY));
		differenceInSeconds -= days * SECONDS_IN_A_DAY;
		int hours = (int) (differenceInSeconds / (SECONDS_IN_AN_HOUR));
		differenceInSeconds -= hours * SECONDS_IN_AN_HOUR;
		int minutes = (int) (differenceInSeconds / (SECONDS_IN_A_MINUTE));
		differenceInSeconds -= minutes * SECONDS_IN_A_MINUTE;
		int seconds = (int) (differenceInSeconds);

		String intervalString = "P" + days + "DT" + hours + "H" + minutes + "M" + seconds + "S";
		return intervalString;
	}
	private static final int SECONDS_IN_A_MINUTE = 60;
	private static final int SECONDS_IN_AN_HOUR = 60 * 60;
	private static final int SECONDS_IN_A_DAY = 24 * 60 * 60;

	/**
	 *
	 * @param interval the Duration to convert to DBDuration's string format
	 * @return the DBDuration equivalent of the Duration value
	 */
	public static String getDurationString(Duration interval) {
		if (interval == null) {
			return null;
		}
		int days = (int) interval.toDaysPart();
		int hours = interval.toHoursPart();
		int minutes = interval.toMinutesPart();

		int nanos = interval.toNanosPart();
		double seconds = interval.toSecondsPart() + (nanos / 1000000000.0);
		String intervalString = "P" + days + "DT" + hours + "H" + minutes + "M" + seconds + "S";
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
		return compareDurationStrings(original, compareTo) == 0;
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
		return compareDurationStrings(original, compareTo) == 1;
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
		return compareDurationStrings(original, compareTo) == -1;
	}

	private static final RegexSplitter SPLIT_ON_LETTERS = Regex.empty().beginSetIncluding().includeLetters().endSet().toSplitter();

	/**
	 *
	 * @param original the first date
	 * @param compareTo the second date
	 *
	 * @return -1 if the first DateRepeat is the smallest, 0 if they are equal,
	 * and 1 if the first is the largest.
	 */
	public static Integer compareDurationStrings(String original, String compareTo) {
		if (original == null || compareTo == null) {
			return null;
		}
		String[] splitOriginal = SPLIT_ON_LETTERS.split(original);
		String[] splitCompareTo = SPLIT_ON_LETTERS.split(compareTo);
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
	public static LocalDateTime addDateAndDurationString(LocalDateTime original, String intervalStr) {
		if (original == null || intervalStr == null || intervalStr.length() == 0 || original.toString().length() == 0) {
			return null;
		}
		int days = getDayPart(intervalStr);
		int hours = getHourPart(intervalStr);
		int minutes = getMinutePart(intervalStr);
		int seconds = getSecondPart(intervalStr);

		int nanos = getNanoPart(intervalStr);

		LocalDateTime cal = LocalDateTime.from(original);
		cal.plusDays(days);
		cal.plusHours(hours);
		cal.plusMinutes(minutes);
		cal.plusSeconds(seconds);
		cal.plusNanos(nanos);
		return cal;
	}

	/**
	 *
	 * @param original the first date.
	 * @param intervalInput the DateRepeat to offset the date.
	 *
	 * @return the Date shift backwards (towards the past) by the DateRepeat
	 * value.
	 */
	public static LocalDateTime subtractDateAndDurationString(LocalDateTime original, String intervalInput) {
		if (original == null || intervalInput == null || intervalInput.length() == 0) {
			return null;
		}

		int days = getDayPart(intervalInput);
		int hours = getHourPart(intervalInput);
		int minutes = getMinutePart(intervalInput);
		int seconds = getSecondPart(intervalInput);
		int nanos = getNanoPart(intervalInput);

		LocalDateTime cal = LocalDateTime.from(original);
		cal.minusDays(days);
		cal.minusHours(hours);
		cal.minusMinutes(minutes);
		cal.minusSeconds(seconds);
		cal.minusNanos(nanos);
		return cal;
	}

	/**
	 *
	 * @param intervalStr the DateRepeat to parse
	 *
	 * @return the DateRepeat value represented by the String value
	 */
	public static Duration parseDateRepeatFromGetString(String intervalStr) {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}

		Duration interval = Duration.parse(intervalStr);
		return interval;
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the fractional seconds to millisecond precision
	 * @throws NumberFormatException it's possible that the numbers are malformed 
	 */
	public static Integer getNanoPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0) {
			return null;
		}
		final String stringValue = GET_SECONDS.getValueFrom(intervalStr).orElse("");
		if (stringValue.isEmpty()) {
			return null;
		}
		final Double secondsDouble = Double.parseDouble(stringValue);
		final int secondsInt = secondsDouble.intValue();
		final Double nanoDouble = secondsDouble * 1000000000.0 - secondsInt * 1000000000;
		final int nanos = nanoDouble.intValue();
		return nanos;
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the integer and fractional seconds part of the DateRepeat
	 * @throws NumberFormatException it's possible that the numbers are malformed 
	 */
	public static Integer getSecondPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0 || !REGEX.matchesEntireString(intervalStr)) {
			return null;
		}
		final String stringValue = GET_SECONDS.getValueFrom(intervalStr).orElse("");
		if (stringValue.isEmpty()) {
			return null;
		}
		final Double valueOf = Double.valueOf(stringValue);
		return valueOf.intValue();
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the minutes part of the DateRepeat
	 * @throws NumberFormatException it's possible that the numbers are malformed 
	 */
	public static Integer getMinutePart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0 || !REGEX.matchesEntireString(intervalStr)) {
			return null;
		}
		final String stringValue = GET_MINUTES.getValueFrom(intervalStr).orElse("");
		if (stringValue.isEmpty()) {
			return null;
		}
		return Integer.parseInt(stringValue);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the hour part of the DateRepeat value
	 * @throws NumberFormatException it's possible that the numbers are malformed 
	 */
	public static Integer getHourPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0 || !REGEX.matchesEntireString(intervalStr)) {
			return null;
		}
		final String stringValue = GET_HOURS.getValueFrom(intervalStr).orElse("");
		if (stringValue.isEmpty()) {
			return null;
		}
		return Integer.parseInt(stringValue);
	}

	/**
	 *
	 * @param intervalStr the DateRepeat
	 *
	 * @return get the day part of the DateRepeat value
	 * @throws NumberFormatException it's possible that the numbers are malformed 
	 */
	public static Integer getDayPart(String intervalStr) throws NumberFormatException {
		if (intervalStr == null || intervalStr.length() == 0 || !REGEX.matchesEntireString(intervalStr)) {
			return null;
		}
		final String stringValue = GET_DAYS.getValueFrom(intervalStr).orElse("");
		if (stringValue.isEmpty()) {
			return null;
		}
		return Integer.parseInt(stringValue);
	}
}
