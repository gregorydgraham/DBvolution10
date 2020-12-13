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
				= allowedValue.add(Regex.startingAnywhere().dot().digits()
				).onceOrNotAtAll();

		final Regex separator
				= new Regex.Range('0', '9')
						.includeMinus()
						.negated()
						.atLeastOnce();

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
		//(-?[0-9]+)([^-0-9]+)(-?[0-9]+):(-?[0-9]+):(-?[0-9]+)(\.\d+)?

		Regex pattern
				= Regex.startingAnywhere()
						.literal('-').onceOrNotAtAll()
						.anyBetween('0', '9').atLeastOnce()
						.add(new Regex.Range('0', '9')
										.includeMinus()
										.negated()
						).atLeastOnce()
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

}
