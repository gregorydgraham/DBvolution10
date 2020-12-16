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

import nz.co.gregs.dbvolution.utility.Regex.RangeBuilder;
import static nz.co.gregs.dbvolution.utility.Regex.startingAnywhere;

/**
 *
 * @author gregorygraham
 * @param <REGEX>
 */
public interface HasRegexFunctions<REGEX extends HasRegexFunctions<REGEX>> {

	public String getRegex();

	/**
	 * Adds a check for a positive or negative integer to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * Will capture the plus or minus so watch out for that in your calculator
	 * application.
	 *
	 * @return a new regexp
	 */
	public default REGEX integer() {
		return extend(Regex
				.startGroup().literal("-")
				.or().literal("+")
				.closeGroup().onceOrNotAtAll()
				.anyBetween('1', '9').once().digit().zeroOrMore()
		);
	}

	/**
	 * Adds a standard pattern that will match any valid number to the pattern as
	 * a grouped element.
	 *
	 * <p>
	 * A valid number is any sequence of digits not starting with zero, optionally
	 * preceded with a plus or minus, and optionally followed by a decimal point
	 * and a sequence of digits, that is clearly separated from other characters.
	 *
	 * <p>
	 * An example of a valid number would be +2.345.
	 *
	 * <p>
	 * Invalid numbers include 02.345, A4, _234, 2*E10, and 5678ABC.
	 *
	 * @return the current regex with a number matching pattern added to it
	 */
	public default REGEX number() {
		return extend(
				Regex.startGroup()
						.anyOf("-+").onceOrNotAtAll()
						.wordBoundary()
						.anyBetween('1', '9').atLeastOnce()
						.digit().zeroOrMore()
						.add(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
						).onceOrNotAtAll()
						.notFollowedBy(Regex.startingAnywhere().nonWhitespace())
						.closeGroup()
		);
	}

	/**
	 * Adds a standard pattern that will match any number-like sequence to the
	 * pattern as a grouped element.
	 *
	 * <p>
	 * A number-like sequence is any sequence of digits, optionally preceded with
	 * a plus or minus, and optionally followed by a decimal point and a sequence
	 * of digits.
	 *
	 * <p>
	 * It differs from a number in that zero can be the first digit and the
	 * sequence doesn't need to be clearly separated from the surrounding
	 * characters.
	 *
	 * <p>
	 * It differs from digits in that leading +/- and a middle decimal point are
	 * included.
	 *
	 * <p>
	 * A valid match would occur for the following +2.345, 02.345, A4, _234,
	 * _234.5, 2*E10, and 5678ABC.
	 *
	 * @return the current regex with a number matching pattern added to it
	 */
	public default REGEX numberLike() {
		return extend(
				Regex.startGroup()
						.anyOf("-+").onceOrNotAtAll()
						.digit().atLeastOnce()
						.add(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
						).onceOrNotAtAll()
						.closeGroup()
		);
	}

	/**
	 * Adds a standard pattern that will match any valid number to the pattern as
	 * a grouped element.
	 *
	 * <p>
	 * A valid number is any sequence of digits not starting with zero, optionally
	 * preceded with a plus or minus, and optionally followed by a decimal point
	 * and a sequence of digits, that is clearly separated from other characters.
	 *
	 * <p>
	 * Examples of a valid number would be +2.345, 2E10, -2.89E-7.98, or 1.37e+15.
	 *
	 * <p>
	 * Invalid numbers include 02.345, A4, _234, 2*E10, and 5678ABC.
	 *
	 * @return the current regex with a number matching pattern added to it
	 */
	public default REGEX numberIncludingScientificNotation() {
		return extend(
				Regex.startGroup()
						.anyOf("-+").onceOrNotAtAll()
						.wordBoundary()
						.anyBetween('1', '9').atLeastOnce()
						.digit().zeroOrMore()
						.add(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
						).onceOrNotAtAll()
						.add(startingAnywhere()
								.literalCaseInsensitive('E').once()
								.anyOf("-+").onceOrNotAtAll()
								.anyBetween('1', '9').atLeastOnce()
								.digit().zeroOrMore()
								.add(startingAnywhere()
										.dot().once()
										.digit().oneOrMore()
								).onceOrNotAtAll()
						).onceOrNotAtAll()
						.notFollowedBy(Regex.startingAnywhere().nonWhitespace())
						.closeGroup()
		);
	}

	REGEX add(HasRegexFunctions<?> second);

	REGEX addGroup(HasRegexFunctions<?> second);

	REGEX anyBetween(Character lowest, Character highest);

	REGEX anyCharacter();

	REGEX anyOf(String literals);

	REGEX asterisk();

	REGEX atLeastOnce();

	REGEX atLeastThisManyTimes(int x);

	REGEX atLeastXAndNoMoreThanYTimes(int x, int y);

	REGEX backslash();

	REGEX bell();

	REGEX bracket();

	REGEX capture(Regex regexp);

	REGEX carat();

	REGEX carriageReturn();

	REGEX controlCharacter(String x);

	REGEX digit();

	REGEX digits();

	REGEX dollarSign();

	REGEX dot();

	default REGEX endOfInput(){
		return endOfTheString();
	}

	REGEX endOfTheString();

	REGEX escapeCharacter();

	REGEX extend(HasRegexFunctions<?> second);

	REGEX formfeed();

	REGEX gapBetweenWords();

	REGEX groupEverythingBeforeThis();

	REGEX literal(String literals);

	REGEX literal(Character character);

	REGEX literalCaseInsensitive(String literals);

	default REGEX literalCaseInsensitive(Character literal) {
		return literalCaseInsensitive("" + literal);
	}

	REGEX negativeInteger();

	REGEX newline();
	
	RegexGroup.NamedCapture<?> namedCapture(String name);

	REGEX nonWhitespace();

	REGEX nonWordBoundary();

	REGEX nonWordCharacter();

	REGEX nondigit();

	REGEX nondigits();

	REGEX noneOf(String literals);

	REGEX notFollowedBy(String literalValue);

	REGEX notFollowedBy(Regex literalValue);

	REGEX notPrecededBy(String literalValue);

	REGEX notPrecededBy(Regex literalValue);

	REGEX nothingBetween(Character lowest, Character highest);

	REGEX once();

	REGEX onceOrNotAtAll();

	REGEX oneOrMore();

	/**
	 * Starts making a character range, use {@link RangeBuilder#closeRange() } to
	 * return to the regex.
	 *
	 * <p>
	 * This provides more options than the {@link #anyBetween(java.lang.Character, java.lang.Character)
	 * } and {@link #anyOf(java.lang.String) } methods for creating ranges.
	 *
	 * @param lowest
	 * @param highest
	 * @return the start of a range.
	 */
	RangeBuilder<REGEX> openRange(char lowest, char highest);

	/**
	 * Starts making a character range, use {@link RangeBuilder#closeRange() } to
	 * return to the regex.
	 *
	 * <p>
	 * This provides more options than the {@link #anyBetween(java.lang.Character, java.lang.Character)
	 * } and {@link #anyOf(java.lang.String) } methods for creating ranges.
	 *
	 * @param literals
	 * @return the start of a range.
	 */
	RangeBuilder<REGEX> openRange(String literals);

	REGEX optionalMany();

	REGEX pipe();

	REGEX plus();

	REGEX positiveInteger();

	/**
	 * Extends this regular expression with an OR grouping.
	 *
	 * <p>
	 * for instance, use this to generate "(FRED|EMILY|GRETA|DONALD)".
	 *
	 * <p>
	 * {@code Regex regex =  Regex.startAnywhere().literal("Project ").startGroup().literal("A").or().literal("B").closeGroup();
	 * } produces "Project (A|B)".
	 *
	 * @return a new regular expression
	 */
	@SuppressWarnings("unchecked")
	public default RegexGroup.Or<REGEX> openGroup() {
		return new RegexGroup.Or<>((REGEX)this);
	}

	REGEX questionMark();

	REGEX space();

	REGEX squareBracket();

	REGEX star();

	REGEX tab();

	REGEX theBeginningOfTheInput();

	REGEX theEndOfTheInput();

	REGEX theEndOfTheInputButForTheFinalTerminator();

	REGEX theEndOfThePreviousMatch();

	REGEX thisManyTimes(int x);

	REGEX unescaped(String unescapedSequence);

	REGEX whitespace();

	REGEX word();

	REGEX wordBoundary();

	REGEX wordCharacter();

	REGEX zeroOrMore();

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
	public default REGEX zeroOrOnce() {
		return onceOrNotAtAll();
	}

}
