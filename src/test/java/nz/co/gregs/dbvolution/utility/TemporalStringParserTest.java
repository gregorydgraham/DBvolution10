/*
 * Copyright 2020 Gregory Graham.
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class TemporalStringParserTest {

	public TemporalStringParserTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testToInstant() throws Exception {
		var originalDate = OffsetDateTime.of(2013, 3, 23, 12, 34, 56, 0, ZoneOffset.UTC);
		var instantVersion = Instant.from(originalDate);
		final String simplePattern = "yyyy-MM-dd HH:mm:ss.SZ";
		final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(simplePattern);
		String formatted = formatter.format(originalDate);
		System.out.println("FORMATTED: "+formatted);
		OffsetDateTime parsed = OffsetDateTime.parse(formatted, formatter);
		Assert.assertThat(parsed, is(originalDate));
//		System.out.println("FORMATTED: "+formatted);
		System.out.println("ORIGINAL: "+instantVersion);
		Instant parsedVersion = TemporalStringParser.toInstant(formatted);
		System.out.println("PARSED: "+instantVersion);
		Assert.assertThat(parsedVersion, is(instantVersion));
//		Assert.assertThat(TemporalStringParser.toInstant("2013-03-23 12:34:56.0 +0:00"), is(instantVersion));
	}

	@Test
	public void testToZonedDateTime() {
	}

	@Test
	public void testPrintException() {
	}

	@Test
	public void testToLocalDateTime() throws Exception {
		var originalDate = LocalDateTime.of(2013, 3, 23, 12, 34, 56, 0);
		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S";
		final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(simplePattern);
		String formatted = formatter.format(originalDate);
		System.out.println("FORMATTED: "+formatted);
		LocalDateTime parsed = LocalDateTime.parse(formatted, formatter);
		Assert.assertThat(parsed, is(originalDate));
//		System.out.println("FORMATTED: "+formatted);
		System.out.println("ORIGINAL: "+originalDate);
		LocalDateTime parsedVersion = TemporalStringParser.toLocalDateTime(formatted);
		System.out.println("PARSED: "+parsedVersion);
		Assert.assertThat(parsedVersion, is(originalDate));
	}

	@Test
	public void testOracleZuluDate() throws Exception {
		String oracleZuluDate = "2011-04-01 12:02:03.0 +00:00";
//		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S X";
//		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S XX";
		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S XXX"; /*Works for +00:00, +01:00*/
//		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S XXXX";
//		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S O";
//		final String simplePattern = "yyyy-MM-dd HH:mm:ss.S OOOO";
		final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(simplePattern);
		OffsetDateTime parsed = OffsetDateTime.parse(oracleZuluDate, formatter);
		System.out.println("PARSED: "+parsed);

		oracleZuluDate = "2011-04-01 12:02:03.0 +01:00";
		parsed = OffsetDateTime.parse(oracleZuluDate, formatter);
		System.out.println("PARSED: "+parsed);

		oracleZuluDate = "2011-04-01 12:02:03.0 +1:00";
		parsed = TemporalStringParser.toOffsetDateTime(oracleZuluDate);
		System.out.println("PARSED: "+parsed);
		Assert.assertThat(parsed, is(OffsetDateTime.of(2011, 4, 1, 12, 2, 3, 0, ZoneOffset.ofHours(1))));

		oracleZuluDate = "2011-04-01 12:02:03.0 +0:00";
		parsed = TemporalStringParser.toOffsetDateTime(oracleZuluDate);
		System.out.println("PARSED: "+parsed);
		Assert.assertThat(parsed, is(OffsetDateTime.of(2011, 4, 1, 12, 2, 3, 0, ZoneOffset.UTC)));
	}

	@Test
	public void testToDate() throws Exception {
	}

}
