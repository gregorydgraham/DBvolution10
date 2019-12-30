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

import java.util.Map;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class SeparatedStringTest {

	public SeparatedStringTest() {
	}

	@Test
	public void testSimpleParsing() {
		final SeparatedString sepString = SeparatedString.byCommas();
		sepString.addAll("aaa", "bbb", "ccc");
		final String encoded = sepString.toString();
		Assert.assertThat(encoded, is("aaa,bbb,ccc"));

		String[] parsed = sepString.parseToArray(encoded);
		assertThat(parsed.length, is(3));
		assertThat(parsed[0], is("aaa"));
		assertThat(parsed[1], is("bbb"));
		assertThat(parsed[2], is("ccc"));
	}

	@Test
	public void testCommaSpaceParsing() {
		final SeparatedString sepString = SeparatedString.byCommaSpace();
		sepString.addAll("aaa", "bbb", "ccc");
		final String encoded = sepString.toString();
		Assert.assertThat(encoded, is("aaa, bbb, ccc"));

		String[] parsed = sepString.parseToArray(encoded);
		assertThat(parsed.length, is(3));
		assertThat(parsed[0], is("aaa"));
		assertThat(parsed[1], is("bbb"));
		assertThat(parsed[2], is("ccc"));
	}

	@Test
	public void testQuotedParsing() {
		// 1000,117090058,117970084,"170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze",Flensburg-Weiche - Flensb. Gr
		String[] parsed = SeparatedString
				.byCommasWithQuotedTermsAndBackslashEscape()
				.parseToArray("1000,117090058,117970084,\"170,9 + 58\",\"179,7 + 84\",\"Flensburg Weiche, W 203 - Flensburg Grenze\",Flensburg-Weiche - Flensb. Gr");
//		Arrays.asList(parsed).stream().forEach((x) -> System.out.println(x));
		assertThat(parsed.length, is(7));
		assertThat(parsed[0], is("1000"));
		assertThat(parsed[1], is("117090058"));
		assertThat(parsed[2], is("117970084"));
		assertThat(parsed[3], is("170,9 + 58"));
		assertThat(parsed[4], is("179,7 + 84"));
		assertThat(parsed[5], is("Flensburg Weiche, W 203 - Flensburg Grenze"));
		assertThat(parsed[6], is("Flensburg-Weiche - Flensb. Gr"));
	}

	@Test
	public void testCustomParsing() {
		final SeparatedString sepString = SeparatedString
				.byTabs()
				.withWrapBefore("~\"")
				.withWrapAfter("\"~")
				.withEscapeChar("==");
// 1000,117090058,117970084,"170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze",Flensburg-Weiche - Flensb. Gr
		sepString.addAll("1000","117090058","117970084","170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze","Flensburg-Weiche - Flensb. Gr", "Albert \"The Pain\" Hallsburg");
		final String encoded = sepString.toString();
		Assert.assertThat(encoded, is("~\"1000\"~\t~\"117090058\"~\t~\"117970084\"~\t~\"170,9 + 58\"~\t~\"179,7 + 84\"~\t~\"Flensburg Weiche, W 203 - Flensburg Grenze\"~\t~\"Flensburg-Weiche - Flensb. Gr\"~\t~\"Albert \"The Pain\" Hallsburg\"~"));

		String[] parsed = sepString.parseToArray(encoded);
//		Arrays.asList(parsed).stream().forEach((x) -> System.out.println(x));
		assertThat(parsed.length, is(8));
		assertThat(parsed[0], is("1000"));
		assertThat(parsed[1], is("117090058"));
		assertThat(parsed[2], is("117970084"));
		assertThat(parsed[3], is("170,9 + 58"));
		assertThat(parsed[4], is("179,7 + 84"));
		assertThat(parsed[5], is("Flensburg Weiche, W 203 - Flensburg Grenze"));
		assertThat(parsed[6], is("Flensburg-Weiche - Flensb. Gr"));
		assertThat(parsed[7], is("Albert \"The Pain\" Hallsburg"));
	}

	@Test
	public void testCustomParsingWithRegexCharacters() {
		final SeparatedString sepString = SeparatedString
				.byTabs()
				.withWrapBefore("+")
				.withWrapAfter("+")
				.withEscapeChar("||");
// 1000,117090058,117970084,"170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze",Flensburg-Weiche - Flensb. Gr
		sepString.addAll("1000","117090058","117970084","170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze","Flensburg-Weiche - Flensb. Gr", "Albert \"The Pain\" Hallsburg");
		final String encoded = sepString.toString();
		Assert.assertThat(encoded, is("+1000+\t+117090058+\t+117970084+\t+170,9 ||+ 58+\t+179,7 ||+ 84+\t+Flensburg Weiche, W 203 - Flensburg Grenze+\t+Flensburg-Weiche - Flensb. Gr+\t+Albert \"The Pain\" Hallsburg+"));

		String[] parsed = sepString.parseToArray(encoded);
//		Arrays.asList(parsed).stream().forEach((x) -> System.out.println(x));
		assertThat(parsed.length, is(8));
		assertThat(parsed[0], is("1000"));
		assertThat(parsed[1], is("117090058"));
		assertThat(parsed[2], is("117970084"));
		assertThat(parsed[3], is("170,9 + 58"));
		assertThat(parsed[4], is("179,7 + 84"));
		assertThat(parsed[5], is("Flensburg Weiche, W 203 - Flensburg Grenze"));
		assertThat(parsed[6], is("Flensburg-Weiche - Flensb. Gr"));
		assertThat(parsed[7], is("Albert \"The Pain\" Hallsburg"));
	}

	@Test
	public void testWithWrappingCustomParsing() {
		final SeparatedString sepString = SeparatedString
				.byTabs()
				.withWrapBefore("~\"")
				.withWrapAfter("\"~")
				.withEscapeChar("==")
				.withPrefix("START")
				.withSuffix("END");
// 1000,117090058,117970084,"170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze",Flensburg-Weiche - Flensb. Gr
		sepString.addAll("1000","117090058~\"","117970084","170,9 + 58","179,7 + 84","Flensburg Weiche, W 203 - Flensburg Grenze","Flensburg-Weiche - Flensb. Gr", "Albert \"The Pain\" Hallsburg");
		final String encoded = sepString.encode();
		Assert.assertThat(encoded, is("START~\"1000\"~\t~\"117090058==~\"\"~\t~\"117970084\"~\t~\"170,9 + 58\"~\t~\"179,7 + 84\"~\t~\"Flensburg Weiche, W 203 - Flensburg Grenze\"~\t~\"Flensburg-Weiche - Flensb. Gr\"~\t~\"Albert \"The Pain\" Hallsburg\"~END"));

		String[] parsed = sepString.parseToArray(encoded);
//		Arrays.asList(parsed).stream().forEach((x) -> System.out.println(x));
		assertThat(parsed.length, is(8));
		assertThat(parsed[0], is("1000"));
		assertThat(parsed[1], is("117090058~\""));
		assertThat(parsed[2], is("117970084"));
		assertThat(parsed[3], is("170,9 + 58"));
		assertThat(parsed[4], is("179,7 + 84"));
		assertThat(parsed[5], is("Flensburg Weiche, W 203 - Flensburg Grenze"));
		assertThat(parsed[6], is("Flensburg-Weiche - Flensb. Gr"));
		assertThat(parsed[7], is("Albert \"The Pain\" Hallsburg"));
	}

	@Test
	public void testEscapeParsing() {
		String[] parsed = SeparatedString
				.byCommasWithQuotedTermsAndBackslashEscape()
				.parseToArray("aaa,\"b\\\"b\\\"b\",c\\,cc");
		assertThat(parsed.length, is(3));
		assertThat(parsed[0], is("aaa"));
		assertThat(parsed[1], is("b\"b\"b"));
		assertThat(parsed[2], is("c,cc"));
	}

	@Test
	public void testMapParsing() {
		Map<String, String> parsed = SeparatedString
				.byCommasWithQuotedTermsAndBackslashEscape()
				.parseToMap("left=10px,right=20em,border=3");
		assertThat(parsed.size(), is(3));
		assertThat(parsed.get("left=10px"), isEmptyString());
		assertThat(parsed.get("right=20em"), isEmptyString());
		assertThat(parsed.get("border=3"), isEmptyString());
		
		parsed = SeparatedString
				.byCommasWithQuotedTermsAndBackslashEscape()
				.withKeyValueSeparator("=")
				.parseToMap("left=10px,right=20em,border=3");
		assertThat(parsed.size(), is(3));
		assertThat(parsed.get("left"), is("10px"));
		assertThat(parsed.get("right"), is("20em"));
		assertThat(parsed.get("border"), is("3"));
	}

}
