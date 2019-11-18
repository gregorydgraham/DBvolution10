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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Encapsulates robust date-time parsing.
 *
 * @author gregorygraham
 */
public class TemporalStringParser {

	static final Log LOG = LogFactory.getLog(TemporalStringParser.class);

	private TemporalStringParser() {
	}

	static final DateTimeFormatter[] INSTANT_FORMATTERS
			= new DateTimeFormatter[]{
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSVV"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSv"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSz"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSVV"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSv"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SVV"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.Sv"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SX"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SZ"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.Sz"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssv"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz"),
				DateTimeFormatter.ISO_OFFSET_DATE_TIME,
				DateTimeFormatter.ISO_ZONED_DATE_TIME,
				DateTimeFormatter.RFC_1123_DATE_TIME,
				DateTimeFormatter.ISO_INSTANT
			};

	static final DateTimeFormatter[] LOCALDATE_FORMATTERS
			= new DateTimeFormatter[]{
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
				DateTimeFormatter.ISO_DATE_TIME,
				DateTimeFormatter.ISO_LOCAL_DATE_TIME,
				DateTimeFormatter.BASIC_ISO_DATE};

	public static Instant toInstant(String inputFromResultSet) throws ParseException {
		return toZonedDateTime(inputFromResultSet).toInstant();
	}

	public static ZonedDateTime toZonedDateTime(String inputFromResultSet) throws ParseException {
		if (inputFromResultSet == null) {
			return null;
		}
		ZonedDateTime zoneddatetime = null;
		String str = inputFromResultSet;
		ParseException exception = new ParseException(str, 0);
		str = inputFromResultSet.replaceFirst(" ", "T");
		final CharSequence sequence = str.subSequence(0, str.length());
		for (DateTimeFormatter format : INSTANT_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(sequence);
				zoneddatetime = ZonedDateTime.from(parsed);
				LOG.debug("PARSE SUCCEEDED: " + format.toString());
				LOG.debug("PARSED: " + sequence);
				LOG.debug("TO: " + zoneddatetime);
			} catch (Exception ex1) {
				LOG.debug("PARSE FAILED: " + format.toString());
				LOG.debug("MESSAGE: " + ex1.getMessage());
				if (ex1 instanceof ParseException) {
					exception = (ParseException) ex1;
				}
			}
		}
		for (DateTimeFormatter format : LOCALDATE_FORMATTERS) {
			try {
				final TemporalAccessor parsed = format.parse(sequence);
				zoneddatetime = ZonedDateTime.of(LocalDateTime.from(parsed), ZoneId.of("Z"));
				LOG.debug("PARSE SUCCEEDED: " + format.toString());
				LOG.debug("PARSED: " + sequence);
				LOG.debug("TO: " + zoneddatetime);
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
			zoneddatetime = ZonedDateTime.of(timestamp.toLocalDateTime(), ZoneId.of("Z"));
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(str)");
			LOG.debug("PARSED: " + str);
			LOG.debug("TO: " + zoneddatetime);
		} catch (Exception ex1) {
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		str = inputFromResultSet;
		try {
			Timestamp timestamp = Timestamp.valueOf(str);
			zoneddatetime = ZonedDateTime.of(timestamp.toLocalDateTime(), ZoneId.of("Z"));
			LOG.debug("PARSE SUCCEEDED: Timestamp.valueOf(str)");
			LOG.debug("PARSED: " + str);
			LOG.debug("TO: " + zoneddatetime);
		} catch (Exception ex1) {
			LOG.debug("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			LOG.debug("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		if (zoneddatetime != null) {
			return zoneddatetime;
		} else {
			LOG.debug("PARSE FAILED:");
			LOG.debug("PARSED: " + inputFromResultSet);
			throw exception;
		}
	}

	public static LocalDateTime toLocalDateTime(String inputFromResultSet) throws ParseException {
		if (inputFromResultSet == null) {
			return null;
		}
		LocalDateTime localdatetime = null;
		String str = inputFromResultSet;
		ParseException exception = new ParseException(str, 0);
		str = inputFromResultSet.replaceFirst(" ", "T");
		final CharSequence sequence = str.subSequence(0, str.length());
		for (DateTimeFormatter format : INSTANT_FORMATTERS) {
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
		for (DateTimeFormatter format : LOCALDATE_FORMATTERS) {
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
		str = inputFromResultSet;
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
			LOG.debug("PARSED: " + inputFromResultSet);
			throw exception;
		}
	}
	
	public static Date toDate(String input) throws ParseException{
		return Date.from( toLocalDateTime(input).atZone( ZoneId.systemDefault()).toInstant());
	}

}
