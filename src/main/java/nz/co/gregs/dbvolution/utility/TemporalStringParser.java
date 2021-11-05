/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import nz.co.gregs.regexi.Regex;
import nz.co.gregs.regexi.internal.RegexReplacement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Encapsulates robust date-time parsing.
 *
 * @author gregorygraham
 */
public class TemporalStringParser {

	static final Log LOG = LogFactory.getLog(TemporalStringParser.class);

	static final Parser[] INSTANT_FORMATTERS = Parser.generateInstantParsers();

	static final Parser[] LOCALDATE_FORMATTERS = Parser.generateLocalDateParsers();

	private TemporalStringParser() {
	}

	public static OffsetDateTime toOffsetDateTime(String inputDateString, String... expectedFormat) {
		final ZonedDateTime toZonedDateTime = toZonedDateTime(inputDateString, Parser.ofPatterns(expectedFormat));
		return toZonedDateTime == null ? null : toZonedDateTime.toOffsetDateTime();
	}

	public static Instant toInstant(String inputDateString, String... expectedFormat) throws DateTimeParseException {
		final ZonedDateTime toZonedDateTime = toZonedDateTime(inputDateString, Parser.ofPatterns(expectedFormat));
		return toZonedDateTime == null ? null : toZonedDateTime.toInstant();
	}

	public static Date toDate(String inputDateString, String... expectedFormat) throws DateTimeParseException {
		final LocalDateTime toLocalDateTime = toLocalDateTime(inputDateString, Parser.ofPatterns(expectedFormat));
		if (toLocalDateTime == null) {
			return null;
		}
		return Date.from(toLocalDateTime.atZone(ZoneId.systemDefault()).toInstant());
	}

	public static ZonedDateTime toZonedDateTime(String inputDateString, String... expectedFormat) throws DateTimeParseException {
		final ZonedDateTime toZonedDateTime = toZonedDateTime(inputDateString, Parser.ofPatterns(expectedFormat));
		return toZonedDateTime;
	}

	private static ZonedDateTime toZonedDateTime(String inputDateString, Parser... preferredFormats) throws DateTimeParseException {
		if (inputDateString == null) {
			return null;
		}
		ZonedDateTime zoneddatetime = null;
		
		// oracle sometimes produces unpadded time zone offsets
		String parsableString = inputDateString;
		Regex regex = Regex.empty().literal("+").namedCapture("offset").digit().literal(":").digit().digit().endNamedCapture().toRegex();
		RegexReplacement replacer = regex.replaceWith().literal("+0").namedReference("offset");
		parsableString = replacer.replaceFirst(parsableString);
		
		DateTimeParseException exception = new DateTimeParseException("Failed to parse datetime '"+parsableString+"'", parsableString, 0);
		for (Parser preferredFormat : preferredFormats) {
			if (preferredFormat != null && preferredFormat.isNotEmpty()) {
				try {
					return preferredFormat.interpretAsZonedDateTime(parsableString);
				} catch (Exception ex1) {
					printException(parsableString, preferredFormat, exception);
					if (ex1 instanceof DateTimeParseException) {
						exception = (DateTimeParseException) ex1;
					} else {
						exception = new DateTimeParseException("FAILED TO PARSE GENERIC DATETIME", parsableString, 0, ex1);
					}
				}
			}
		}
		for (Parser format : INSTANT_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(parsableString);
				zoneddatetime = ZonedDateTime.from(parsed);
				System.out.println("PARSE SUCCEEDED: " + format.pattern);
				return zoneddatetime;
			} catch (Exception ex1) {
				printException(parsableString, format, exception);
				if (ex1 instanceof DateTimeParseException) {
					exception = (DateTimeParseException) ex1;
				} else {
					exception = new DateTimeParseException("Failed to parse '" + parsableString + "'", parsableString, 0, ex1);
				}
			}
		}
		for (Parser format : LOCALDATE_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(parsableString);
				zoneddatetime = ZonedDateTime.of(LocalDateTime.from(parsed), ZoneId.of("Z"));
				System.out.println("PARSE SUCCEEDED: " + format.pattern);
				return zoneddatetime;
			} catch (Exception ex1) {
				printException(parsableString, format, exception);
				LOG.debug("PARSE FAILED: " + format.toString());
				LOG.debug("MESSAGE: " + ex1.getMessage());
			}
		}
		try {
			Timestamp timestamp = Timestamp.valueOf(parsableString);
			zoneddatetime = ZonedDateTime.of(timestamp.toLocalDateTime(), ZoneId.of("Z"));
			System.out.println("PARSE SUCCEEDED:  Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("PARSED STR: " + parsableString);
			LOG.debug("TO: " + zoneddatetime);
			return zoneddatetime;
		} catch (Exception ex1) {
			printException(parsableString, "Timestamp.valueOf: yyyy-[m]m-[d]d hh:mm:ss[.f...]", exception);
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
		}
		try {
			Timestamp timestamp = Timestamp.valueOf(parsableString);
			zoneddatetime = ZonedDateTime.of(timestamp.toLocalDateTime(), ZoneId.of("Z"));
			System.out.println("PARSE SUCCEEDED:  Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("PARSED inputDateString: " + parsableString);
			LOG.debug("TO: " + zoneddatetime);
			return zoneddatetime;
		} catch (Exception ex1) {
			printException(parsableString, "Timestamp.valueOf: yyyy-[m]m-[d]d hh:mm:ss[.f...]", exception);
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + parsableString + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof DateTimeParseException) {
				exception = (DateTimeParseException) ex1;
			} else {
				exception = new DateTimeParseException("Failed to parse Datetime '"+parsableString+"'", parsableString, 0, ex1);
			}
		}
		if (zoneddatetime != null) {
			return zoneddatetime;
		} else {
			LOG.debug("FAILED TO PARSE DATE");
			LOG.debug("INPUTSTRING: " + parsableString);
			LOG.debug("TEST VERSION: " + parsableString);
			throw exception;
		}
	}

	private static void printException(CharSequence input, Parser format, Exception exception) {
		printException(input.toString(), format, exception);
	}

	private static void printException(String input, Parser format, Exception exception) {
		printException(input, format.toString(), exception);
	}

	private static void printException(String inputDateString, String format, Exception exception) {
		LOG.debug("PARSING ORIGINAL: " + inputDateString);
		LOG.debug("PATTERN: " + format);
		LOG.debug("PARSE FAILED: " + inputDateString);
		LOG.debug("EXCEPTION: " + exception.getMessage());
		StackTraceElement[] stackTrace = exception.getStackTrace();
		for (int i = 0; i < Math.min(10, stackTrace.length); i++) {
			LOG.debug(stackTrace[i].toString());
		}
	}

	public static LocalDate toLocalDate(String inputFromResultSet, String... expectedFormats) throws DateTimeParseException {
		return toLocalDateTime(inputFromResultSet, Parser.ofPatterns(expectedFormats)).toLocalDate();
	}

	public static LocalDateTime toLocalDateTime(String inputFromResultSet, String... expectedFormats) throws DateTimeParseException {
		return toLocalDateTime(inputFromResultSet, Parser.ofPatterns(expectedFormats));
	}

	private static LocalDateTime toLocalDateTime(String inputDateString, Parser... preferredFormats) throws DateTimeParseException {
		if (inputDateString == null) {
			return null;
		}
		LocalDateTime localdatetime = null;
		String str = inputDateString;
		Exception exception = new ParseException(str, 0);
		final CharSequence sequence = str.subSequence(0, str.length());
		for (Parser preferredFormat : preferredFormats) {
			if (preferredFormat != null && preferredFormat.isNotEmpty()) {
				try {
					return preferredFormat.interpretAsLocalDateTime(str);
				} catch (Exception ex1) {
					printException(inputDateString, preferredFormat, exception);
					if (ex1 instanceof DateTimeParseException) {
						exception = ex1;
					} else {
						exception = new DateTimeParseException("FAILED TO PARSE GENERIC DATETIME", str, 0, ex1);
					}
				}
			}
		}
		for (Parser format : INSTANT_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(sequence);
				localdatetime = ZonedDateTime.from(parsed).toLocalDateTime();
				LOG.debug("PARSE SUCCEEDED: " + format.toString());
				LOG.debug("PARSED: " + sequence);
				LOG.debug("TO: " + localdatetime);
			} catch (Exception ex1) {
				LOG.debug("PARSE FAILED: " + format.toString());
				LOG.debug("MESSAGE: " + ex1.getMessage());
				if (ex1 instanceof ParseException) {
					exception = (ParseException) ex1;
				}
			}
		}
		for (Parser format : LOCALDATE_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(sequence);
				localdatetime = LocalDateTime.from(parsed);
				LOG.debug("PARSE SUCCEEDED: " + format.toString());
				LOG.debug("PARSED: " + sequence);
				LOG.debug("TO: " + localdatetime);
			} catch (Exception ex1) {
				LOG.debug("PARSE FAILED: " + format.toString());
				LOG.debug("MESSAGE: " + ex1.getMessage());
				if (ex1 instanceof ParseException) {
					exception = (ParseException) ex1;
				}
			}
		}
		try {
			Timestamp timestamp = Timestamp.valueOf(str);
			localdatetime = timestamp.toLocalDateTime();
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(str)");
			LOG.debug("PARSED: " + str);
			LOG.debug("TO: " + localdatetime);
		} catch (Exception ex1) {
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		str = inputDateString;
		try {
			Timestamp timestamp = Timestamp.valueOf(str);
			localdatetime = timestamp.toLocalDateTime();
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(str)");
			LOG.debug("PARSED: " + str);
			LOG.debug("TO: " + localdatetime);
		} catch (Exception ex1) {
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		if (localdatetime != null) {
			return localdatetime;
		} else {
			LOG.debug("PARSE FAILED:");
			LOG.debug("PARSED: " + inputDateString);
			throw new DateTimeParseException(inputDateString, sequence, 0, exception);
		}
	}

	private static class Parser {

		private final String pattern;
		private final DateTimeFormatter formatter;

		private Parser(String pattern) {
			this.pattern = pattern;
			this.formatter = pattern == null ? null : DateTimeFormatter.ofPattern(pattern);
		}

		private Parser(DateTimeFormatter formatter) {
			this.pattern = formatter.toString();
			this.formatter = formatter;
		}

		public static Parser ofPattern(String pattern) {
			return new Parser(pattern);
		}

		public static Parser[] ofPatterns(String... pattern) {
			List<Parser> list = List.of(pattern).stream().map(p -> new Parser(p)).collect(Collectors.toList());
			Parser[] array = list.toArray(new Parser[]{});
			return array;
		}

		public static Parser ofFormatter(DateTimeFormatter formatter) {
			return new Parser(formatter);
		}

		public boolean isNotEmpty() {
			return StringCheck.isNotEmptyNorNull(pattern);
		}

		@Override
		public String toString() {
			return pattern;
		}

		public ZonedDateTime interpretAsZonedDateTime(String dateString) throws DateTimeParseException {
			final TemporalAccessor parsed = parse(dateString);
			ZonedDateTime zoneddatetime;
			try {
				zoneddatetime = ZonedDateTime.from(parsed);
				return zoneddatetime;
			} catch (Exception ex) {
				try {
					zoneddatetime = ZonedDateTime.of(LocalDateTime.from(parsed), ZoneId.of("Z"));
					return zoneddatetime;
				} catch (Exception ex1) {
					DateTimeParseException exception;
					printException(dateString, this, ex1);
					if (ex1 instanceof DateTimeParseException) {
						exception = (DateTimeParseException) ex1;
					} else {
						exception = new DateTimeParseException("Failed to parse date string", dateString, 0, ex1);
					}
					throw exception;
				}
			}
		}

		public LocalDateTime interpretAsLocalDateTime(String dateString) throws DateTimeParseException {
			final TemporalAccessor parsed = parse(dateString);
			LocalDateTime localdatetime;
			try {
				localdatetime = ZonedDateTime.from(parsed).toLocalDateTime();
				return localdatetime;
			} catch (Exception ex) {
				try {
					localdatetime = LocalDateTime.from(parsed);
					return localdatetime;
				} catch (Exception ex1) {
					DateTimeParseException exception;
					printException(dateString, this, ex1);
					if (ex1 instanceof DateTimeParseException) {
						exception = (DateTimeParseException) ex1;
					} else {
						exception = new DateTimeParseException("Failed to parse date string", dateString, 0, ex1);
					}
					throw exception;
				}
			}
		}

		public TemporalAccessor parse(String dateString) {
			return parse(dateString.subSequence(0, dateString.length()));
		}

		public TemporalAccessor parse(CharSequence dateString) {
			return formatter.parse(dateString);
		}

		static String[] yearParts = new String[]{"uuuu", "yyyy"};
		static String[] monthParts = new String[]{"MM"};
		static String[] dayParts = new String[]{"dd"};
		static String[] dayPartDividerParts = new String[]{"-", ""};
		static String[] dayTimeDividerParts = new String[]{" ", "'T'", ""};
		static String[] timePartDividerParts = new String[]{":", ""};
		static String[] timeTimeZoneDividerParts = new String[]{" ", ""};
		static String[] hourParts = new String[]{"HH"};
		static String[] minuteParts = new String[]{"mm"};
		static String[] secondParts = new String[]{"ss"};
		static String[] subsecondParts = new String[]{"SSSSSSSSS", "SSSSSS", "SSSSS", "SSSS", "SSS", "SS", "S", ""};
		static String[] secondPartDividerParts = new String[]{".", ""};
		static String[] timezoneParts = new String[]{"VV", "zzzz", "OOOO", "XXXXX", "xxxxx", "ZZZZZ", "XXXX", "xxxx", "ZZZZ", "XXX", "xxx", "XX", "xx", "z", "O", "X", "x", "Z"};

		public static Parser[] generateInstantParsers() {
			List<Parser> parsers = new ArrayList<>();
			for (String dayPartDividerPart : dayPartDividerParts) {
				for (String timePartDividerPart : timePartDividerParts) {
					for (String secondPartDividerPart : secondPartDividerParts) {
						for (String yearPart : yearParts) {
							for (String monthPart : monthParts) {
								for (String dayPart : dayParts) {
									for (String dayTimeDividerPart : dayTimeDividerParts) {
										for (String hourPart : hourParts) {
											for (String minutePart : minuteParts) {
												for (String secondPart : secondParts) {
													for (String subsecondPart : subsecondParts) {
														for (String timeTimeZoneDivider : timeTimeZoneDividerParts) {
															for (String timezonePart : timezoneParts) {
																final String pattern = makeInstantPatternFromParts(yearPart, dayPartDividerPart, monthPart, dayPart, dayTimeDividerPart, hourPart, timePartDividerPart, minutePart, secondPart, secondPartDividerPart, subsecondPart, timeTimeZoneDivider, timezonePart);
																parsers.add(Parser.ofPattern(pattern));
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			parsers.add(Parser.ofFormatter(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			parsers.add(Parser.ofFormatter(DateTimeFormatter.ISO_ZONED_DATE_TIME));
			parsers.add(Parser.ofFormatter(DateTimeFormatter.RFC_1123_DATE_TIME));
			parsers.add(Parser.ofFormatter(DateTimeFormatter.ISO_INSTANT));
			return parsers.toArray(new Parser[]{});
		}

		private static String makeInstantPatternFromParts(String yearPart, String dayPartDividerPart, String monthPart, String dayPart, String dayTimeDividerPart, String hourPart, String timePartDividerPart, String minutePart, String secondPart, String secondPartDividerPart, String subsecondPart, String timeTimeZoneDivider, String timezonePart) {
			return makeLocalDatePatternFromParts(yearPart, dayPartDividerPart, monthPart, dayPart, dayTimeDividerPart, hourPart, timePartDividerPart, minutePart, secondPart, secondPartDividerPart, subsecondPart) + timeTimeZoneDivider + timezonePart;
		}

		public static Parser[] generateLocalDateParsers() {
			List<Parser> parsers = new ArrayList<>();
			for (String dayPartDividerPart : dayPartDividerParts) {
				for (String timePartDividerPart : timePartDividerParts) {
					for (String secondPartDividerPart : secondPartDividerParts) {
						for (String yearPart : yearParts) {
							for (String monthPart : monthParts) {
								for (String dayPart : dayParts) {
									for (String dayTimeDividerPart : dayTimeDividerParts) {
										for (String hourPart : hourParts) {
											for (String minutePart : minuteParts) {
												for (String secondPart : secondParts) {
													for (String subsecondPart : subsecondParts) {
														final String pattern = makeLocalDatePatternFromParts(yearPart, dayPartDividerPart, monthPart, dayPart, dayTimeDividerPart, hourPart, timePartDividerPart, minutePart, secondPart, secondPartDividerPart, subsecondPart);
//														System.out.println("PATTERN: " + pattern);
														parsers.add(Parser.ofPattern(pattern));
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			parsers.add(Parser.ofFormatter(DateTimeFormatter.ISO_DATE_TIME));
			parsers.add(Parser.ofFormatter(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
			parsers.add(Parser.ofFormatter(DateTimeFormatter.BASIC_ISO_DATE));
			return parsers.toArray(new Parser[]{});
		}

		private static String makeLocalDatePatternFromParts(String yearPart, String dayPartDividerPart, String monthPart, String dayPart, String dayTimeDividerPart, String hourPart, String timePartDividerPart, String minutePart, String secondPart, String secondPartDividerPart, String subsecondPart) {
			return "" + yearPart + dayPartDividerPart + monthPart + dayPartDividerPart + dayPart + dayTimeDividerPart + hourPart + timePartDividerPart + minutePart + timePartDividerPart + secondPart + secondPartDividerPart + subsecondPart;
		}
	}
}
