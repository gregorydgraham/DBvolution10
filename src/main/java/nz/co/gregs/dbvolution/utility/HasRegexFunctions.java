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

/**
 *
 * @author gregorygraham
 * @param <THIS>
 */
public interface HasRegexFunctions<THIS extends HasRegexFunctions<THIS>> {

	THIS add(Regex second);

	THIS addGroup(Regex second);

	THIS anyBetween(Character lowest, Character highest);

	THIS anyCharacter();

	THIS anyOf(String literals);

	THIS asterisk();

	THIS atLeastOnce();

	THIS atLeastThisManyTimes(int x);

	THIS atLeastXAndNoMoreThanYTimes(int x, int y);

	THIS backslash();

	THIS bell();

	THIS bracket();

	THIS capture(Regex regexp);

	THIS carat();

	THIS carriageReturn();

	THIS controlCharacter(String x);

	THIS digit();

	THIS digits();

	THIS dollarSign();

	THIS dot();

	THIS endOfTheString();

	THIS escapeCharacter();

	THIS extend(Regex second);

	THIS formfeed();

	THIS gapBetweenWords();

	THIS groupEverythingBeforeThis();

	THIS integer();

	THIS literal(String literals);

	THIS literal(Character character);

	THIS literalCaseInsensitive(String literals);

	default THIS literalCaseInsensitive(Character literal) {
		return literalCaseInsensitive("" + literal);
	}

	THIS negativeInteger();

	THIS newline();

	THIS nonWhitespace();

	THIS nonWordBoundary();

	THIS nonWordCharacter();

	THIS nondigit();

	THIS nondigits();

	THIS noneOf(String literals);

	THIS notFollowedBy(String literalValue);

	THIS notFollowedBy(Regex literalValue);

	THIS notPrecededBy(String literalValue);

	THIS notPrecededBy(Regex literalValue);

	THIS nothingBetween(Character lowest, Character highest);

	THIS number();

	THIS numberLike();

	THIS once();

	THIS onceOrNotAtAll();

	THIS oneOrMore();

	THIS optionalMany();
	
	THIS pipe();

	THIS plus();

	THIS positiveInteger();

	THIS questionMark();

	THIS space();

	THIS squareBracket();

	THIS star();

	THIS tab();

	THIS theBeginningOfTheInput();

	THIS theEndOfTheInput();

	THIS theEndOfTheInputButForTheFinalTerminator();

	THIS theEndOfThePreviousMatch();

	THIS thisManyTimes(int x);

	THIS unescaped(String unescapedSequence);

	THIS whitespace();

	THIS word();

	THIS wordBoundary();

	THIS wordCharacter();

	THIS zeroOrMore();

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.onceOrNotAtAll() will match "a" or "ab", but not
	 * "abb"
	 *
	 * @return a new regexp
	 */
	public default THIS zeroOrOnce() {
		return onceOrNotAtAll();
	}

}
