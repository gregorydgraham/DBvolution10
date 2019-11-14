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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * Encapsulates robust date-time parsing.
 *
 * @author gregorygraham
 */
public class TemporalStringParser {

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
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"), 
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"), 
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"), 
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"), 
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"), 
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"), 
				DateTimeFormatter.ISO_OFFSET_DATE_TIME, 
				DateTimeFormatter.ISO_ZONED_DATE_TIME, 
				DateTimeFormatter.RFC_1123_DATE_TIME, 
				DateTimeFormatter.ISO_INSTANT, 
				DateTimeFormatter.ISO_DATE_TIME, 
				DateTimeFormatter.ISO_LOCAL_DATE_TIME, 
				DateTimeFormatter.BASIC_ISO_DATE};

	public static Instant toInstant(String inputFromResultSet) throws ParseException {
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
				return zoneddatetime.toInstant();
			} catch (Exception ex1) {
				System.out.println("PARSE FAILED: " + format.toString());
				System.out.println("MESSAGE: " + ex1.getMessage());
				if (ex1 instanceof ParseException) {
					exception = (ParseException) ex1;
				}
			}
		}
		;
		try {
			Instant instant = Timestamp.valueOf(str).toInstant();
			return instant;
		} catch (Exception ex1) {
			System.out.println("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			System.out.println("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		str = inputFromResultSet;
		try {
			Instant instant = Timestamp.valueOf(str).toInstant();
			return instant;
		} catch (Exception ex1) {
			System.out.println("PARSE FAILED: Timestamp.valueOf(" + str + ")");
			System.out.println("MESSAGE: " + ex1.getMessage());
			if (ex1 instanceof ParseException) {
				exception = (ParseException) ex1;
			}
		}
		if (zoneddatetime != null) {
			return zoneddatetime.toInstant();
		}
		throw exception;
	}

}
