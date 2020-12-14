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

import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class RegexTest {

	public RegexTest() {
	}

	@Test
	public void testFindingANegativeNumber() {
		Regex negativeInteger = Regex.startingAnywhere().negativeInteger();
		Assert.assertTrue(negativeInteger.matchesEntireString("-1"));
		Assert.assertTrue(negativeInteger.matchesWithinString("-1"));
		Assert.assertFalse(negativeInteger.matchesEntireString("1"));
		Assert.assertFalse(negativeInteger.matchesWithinString("1"));
		Assert.assertFalse(negativeInteger.matchesEntireString("below zero there are negative and -1 is the first"));
		Assert.assertTrue(negativeInteger.matchesWithinString("below zero there are negative and -1 is the first"));
	}

	@Test
	public void testFindingAPositiveNumber() {
		Regex positiveInteger = Regex.startingAnywhere().positiveInteger();
		Assert.assertThat(positiveInteger.matchesEntireString("-1"), is(false));
		Assert.assertThat(positiveInteger.matchesWithinString("-1"), is(false));
		Assert.assertThat(positiveInteger.matchesEntireString("1"), is(true));
		Assert.assertThat(positiveInteger.matchesWithinString("1"), is(true));
		Assert.assertThat(positiveInteger.matchesEntireString("+1"), is(true));
		Assert.assertThat(positiveInteger.matchesWithinString("+1"), is(true));
		Assert.assertThat(positiveInteger.matchesEntireString("below zero there are negatives and -1 is the first"), is(false));
		Assert.assertThat(positiveInteger.matchesWithinString("below zero there are negatives and -1 is the first"), is(false));
		Assert.assertThat(positiveInteger.matchesEntireString("above zero there are positives and 1 is the first"), is(false));
		Assert.assertThat(positiveInteger.matchesWithinString("above zero there are positives and 1 is the first"), is(true));
		Assert.assertThat(positiveInteger.matchesEntireString("above zero there are positives and +1 is the first"), is(false));
		Assert.assertThat(positiveInteger.matchesWithinString("above zero there are positives and +1 is the first"), is(true));
	}

	@Test
	public void testFindingPostgresIntervalValues() {
		// -2 days 00:00:00
		// 1 days 00:00:5.5
		// 0 days 00:00:-5.5
		//
		//(-?[0-9]+)([^-0-9]+)(-?[0-9]+):(-?[0-9]+):(-?[0-9]+)(\.\d+)?

		final Regex allowedValue
				= Regex.startingAnywhere()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce();

		final Regex allowedSeconds
				= allowedValue.add(
						Regex.startingAnywhere().dot().digits()
				).onceOrNotAtAll();

		final Regex separator
				= Regex.startingAnywhere().openRange('0', '9').includeMinus().negated().closeRange().atLeastOnce();
//				= new Regex.Range('0', '9')
//						.includeMinus()
//						.negated()
//						.atLeastOnce();

		Regex pattern
				= Regex.startingAnywhere()
						.add(allowedValue).add(separator)
						.add(allowedValue).literal(':')
						.add(allowedValue).literal(':')
						.add(allowedSeconds);

//		System.out.println("PASS: " + pattern.matchesWithinString("-2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00.0"));
//		System.out.println("PASS: " + pattern.matchesWithinString("1 days 00:00:5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("1 days 00:00:5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("0 days 00:00:-5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("0 00:00:-5.5"));
//		System.out.println("FAIL: " + pattern.matchesWithinString("00:00:-5.5"));
//		System.out.println("FAIL: " + pattern.matchesWithinString("-2 days"));
		Assert.assertThat(pattern.matchesWithinString("-2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00.0"), is(true));
		Assert.assertThat(pattern.matchesWithinString("1 days 00:00:5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("1 days 00:00:5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("0 days 00:00:-5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("0 00:00:-5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("00:00:-5.5"), is(false));
		Assert.assertThat(pattern.matchesWithinString("-2"), is(false));

	}

	@Test
	public void testFindingPostgresIntervalValuesWithAOneliner() {
		// -2 days 00:00:00
		// 1 days 00:00:5.5
		// 0 days 00:00:-5.5
		//
		//-?[0-9]+([^-0-9])+-?[0-9]+:{1}-?[0-9]+:{1}-?[0-9]+(\.\d+)?
		Regex pattern
				= Regex.startingAnywhere()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce()
						.openRange('0', '9')
						.includeMinus()
						.negated()
						.closeRange()
						.atLeastOnce()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce()
						.literal(':').once()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce()
						.literal(':').once()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce().add(Regex.startingAnywhere().dot().digits()
				).onceOrNotAtAll();

//		System.out.println("PASS: " + pattern.matchesWithinString("-2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00.0"));
//		System.out.println("PASS: " + pattern.matchesWithinString("1 days 00:00:5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("2 days 00:00:00"));
//		System.out.println("PASS: " + pattern.matchesWithinString("1 days 00:00:5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("0 days 00:00:-5.5"));
//		System.out.println("PASS: " + pattern.matchesWithinString("0 00:00:-5.5"));
//		System.out.println("FAIL: " + pattern.matchesWithinString("00:00:-5.5"));
//		System.out.println("FAIL: " + pattern.matchesWithinString("-2 days"));
		Assert.assertThat(pattern.matchesWithinString("-2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00.0"), is(true));
		Assert.assertThat(pattern.matchesWithinString("1 days 00:00:5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("2 days 00:00:00"), is(true));
		Assert.assertThat(pattern.matchesWithinString("1 days 00:00:5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("0 days 00:00:-5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("0 00:00:-5.5"), is(true));
		Assert.assertThat(pattern.matchesWithinString("00:00:-5.5"), is(false));
		Assert.assertThat(pattern.matchesWithinString("-2"), is(false));

	}

	@Test
	public void testGroupBuilding() {

		Regex pattern
				= Regex.startGroup().literal("Amy").or().literal("Bob").or().literal("Charlie").closeGroup();

		Assert.assertThat(pattern.matchesWithinString("Amy"), is(true));
		Assert.assertThat(pattern.matchesWithinString("Bob"), is(true));
		Assert.assertThat(pattern.matchesWithinString("Charlie"), is(true));
		Assert.assertThat(pattern.matchesEntireString("Amy"), is(true));
		Assert.assertThat(pattern.matchesEntireString("Bob"), is(true));
		Assert.assertThat(pattern.matchesEntireString("Charlie"), is(true));
		Assert.assertThat(pattern.matchesWithinString("David"), is(false));
		Assert.assertThat(pattern.matchesWithinString("Emma"), is(false));
		Assert.assertThat(pattern.matchesWithinString("Try with Amy in the middle"), is(true));
		Assert.assertThat(pattern.matchesWithinString("End it with Bob"), is(true));
		Assert.assertThat(pattern.matchesWithinString("Charlie at the start"), is(true));
		Assert.assertThat(pattern.matchesEntireString("Try with Amy in the middle"), is(false));
		Assert.assertThat(pattern.matchesEntireString("End it with Bob"), is(false));
		Assert.assertThat(pattern.matchesEntireString("Charlie at the start"), is(false));
		Assert.assertThat(pattern.matchesWithinString("Still can't find David"), is(false));
		Assert.assertThat(pattern.matchesWithinString("Emma doesn't do any better"), is(false));

	}

	@Test
	public void testNumberElement() {
		// -2 days 00:00:00
		// 1 days 00:00:5.5
		// 0 days 00:00:-5.5
		//
		// ([-+]?\b[1-9]+\d*(\.{1}\d+)?){1}
		Regex pattern
				= Regex.startingAnywhere()
						.number().once();

//		System.out.println("REGEX: " + pattern.getRegexp());

		Assert.assertThat(pattern.matchesWithinString("before -1 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 2 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -234 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before +4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 4.5 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -4.5 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -4.05 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 02 after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before -0234 after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before 004 after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before _4 after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before A4 after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before A4after"), is(false));
		Assert.assertThat(pattern.matchesWithinString("before 2*E10"), is(false));

	}

	@Test
	public void testNumberLike() {
		// -2 days 00:00:00
		// 1 days 00:00:5.5
		// 0 days 00:00:-5.5
		//
		// ([-+]?\b[1-9]+\d*(\.{1}\d+)?){1}
		Regex pattern
				= Regex.startingAnywhere()
						.numberLike().once();

//		System.out.println("REGEX: " + pattern.getRegexp());

		//-1 2 -234 +4 -4 4.5 FAIL 02 -0234 004 _4 A4
		Assert.assertThat(pattern.matchesWithinString("before -1 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 2 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -234 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before +4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 4.5 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -4.5 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 02 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before -0234 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 004 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before _4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before A4 after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before A4after"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before 2*E10"), is(true));
		Assert.assertThat(pattern.matchesWithinString("before"), is(false));

	}

}
