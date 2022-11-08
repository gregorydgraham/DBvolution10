/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import static org.hamcrest.Matchers.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBPasswordHash;
import nz.co.gregs.dbvolution.datatypes.DBStringTrimmed;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;

import org.junit.Test;

/**
 *
 */
public class MultiplePrimaryKeyTests extends AbstractTest {

	public MultiplePrimaryKeyTests(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void createTables() throws SQLException {
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Colleagues());
			database.createTable(new Colleagues());
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new User());
			database.createTable(new User());
		} catch (SQLException ex) {
			; // An exception is thrown if the table already exists
		}
		User u1 = new User(1);
		User u2 = new User(2);
		User u3 = new User(3);
		User u4 = new User(4);
		database.insert(u1, u2, u3, u4);
		Colleagues t1 = new Colleagues(1, 2);
		Colleagues t2 = new Colleagues(1, 3);
		Colleagues t3 = new Colleagues(2, 4);
		database.insert(t1, t2, t3);
	}

	@Test
	public void testQueryFindsDifferentRows() throws SQLException {
		List<ColleagueListItem> list = new ArrayList<>(0);
		list.clear();
		final Colleagues colleagueExample = new Colleagues();
		final User requesterExample = new RequestingUser();
		final User invitedExample = new InvitedUser();
		DBQuery dbQuery = database.getDBQuery(colleagueExample, requesterExample, invitedExample);
		dbQuery.addCondition(// at least one of the invite fields is the current user
				IntegerExpression.value(1)
						.isIn(
								requesterExample.column(requesterExample.queryUserID()),
								invitedExample.column(invitedExample.queryUserID())
						)
		);

//		System.out.println("getColleaguesToList: \n" + dbQuery.getSQLForQuery());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(2));
		Colleagues colleagueRow1 = allRows.get(0).get(new Colleagues());
		Colleagues colleagueRow2 = allRows.get(1).get(new Colleagues());
		assertThat(colleagueRow1.invited.getValue(), not(is(colleagueRow2.invited.getValue())));

	}

	public static class ColleagueListItem {

		private User otherUser;
		private boolean accepted;
		private boolean canAccept;
		private Colleagues colleaguesRow;
		private boolean declined;

		public ColleagueListItem() {
		}

		public ColleagueListItem(Colleagues colleagues, User requester, User invitedUser) {
			this.colleaguesRow = colleagues;
			if (1 == invitedUser.getUserID()) {
				otherUser = requester;
				canAccept = true;
			} else {
				otherUser = invitedUser;
				canAccept = false;
			}
			accepted = colleagues.acceptanceDate.isNotNull();
			declined = colleagues.denialDate.isNotNull();
//			System.out.println("ColleagueListItem");
		}

		public Colleagues getColleaguesRow() {
			return colleaguesRow;
		}

		public User getOtherUser() {
			return otherUser;
		}

		public boolean hasAcceptedInvitation() {
			return accepted && !hasDeclined();
		}

		public boolean isInvited() {
			return !accepted && !hasDeclined();
		}

		public boolean canAccept() {
			return canAccept;
		}

		public boolean hasDeclined() {
			return declined;
		}

	}

	public static class Colleagues extends DBRow {

	private static final long serialVersionUID = 1L;

		@DBColumn("userid1")
		@DBPrimaryKey
		@DBForeignKey(RequestingUser.class)
		public final DBInteger requestor = new DBInteger();

		@DBColumn("userid2")
		@DBPrimaryKey
		@DBForeignKey(InvitedUser.class)
		public final DBInteger invited = new DBInteger();

		@DBColumn
		public final DBDate invitationDate = new DBDate();

		@DBColumn
		public final DBDate acceptanceDate = new DBDate();

		@DBColumn
		public final DBDate denialDate = new DBDate();

		public Colleagues(User user1, User user2) {
			requestor.setValue(user1.getUserID());
			invited.setValue(user2.getUserID());
			invitationDate.setValue(new Date());
		}

		public Colleagues(int user1, int user2) {
			requestor.setValue(user1);
			invited.setValue(user2);
			invitationDate.setValue(new Date());
		}

		public Colleagues() {
		}
	}

	public static class RequestingUser extends User {

	private static final long serialVersionUID = 1L;

		public RequestingUser(int i) {
			super(i);
		}

		public RequestingUser() {
			super();
		}

	}

	public static class InvitedUser extends User {

	private static final long serialVersionUID = 1L;

		public InvitedUser(int i) {
			super(i);
		}

		public InvitedUser() {
			super();
		}

	}

	@DBTableName("USER_TABLE_FOR_TESTING")
	public static class User extends DBRow {

	private static final long serialVersionUID = 1L;

		public User() {
		}

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		private final DBInteger userID = new DBInteger();

		@DBColumn
		private final DBStringTrimmed username = new DBStringTrimmed();

		@DBColumn
		private final DBStringTrimmed email = new DBStringTrimmed();

		@DBColumn
		private final DBStringTrimmed blurb = new DBStringTrimmed();

		@DBColumn
		private final DBPasswordHash password = new DBPasswordHash();

		@DBColumn
		private final DBDate signupDate = new DBDate();

		@DBColumn
		private final DBDate lastLoginDate = new DBDate();

//	@DBColumn
//	@DBForeignKey(Document.class)
//	private final DBInteger profileImageID = new DBInteger();
		@DBColumn
		public final DBDate createdDate = new DBDate()
				.setDefaultInsertValue(DateExpression.currentDate());

		@DBColumn
		public final DBDate modifiedDate = new DBDate()
				.setDefaultInsertValue(DateExpression.currentDate())
				.setDefaultUpdateValue(DateExpression.currentDate());

		private User(int i) {
			super();
			userID.setValue(i);
		}

//	@AutoFillDuringQueryIfPossible
//	public Document profileImage;
		/**
		 * @return the userID
		 */
		public DBInteger queryUserID() {
			return userID;
		}

		/**
		 * @return the username
		 */
		public DBStringTrimmed queryUsername() {
			return username;
		}

		/**
		 * @return the email
		 */
		public DBStringTrimmed queryEmail() {
			return email;
		}

		/**
		 * @return the password
		 */
		public DBPasswordHash queryPassword() {
			return password;
		}

		/**
		 * @return the signupDate
		 */
		public DBDate querySignupDate() {
			return signupDate;
		}

		/**
		 * @return the lastLoginDate
		 */
		public DBDate queryLastLoginDate() {
			return lastLoginDate;
		}

		/**
		 * @return the userID
		 */
		public Long getUserID() {
			return userID.getValue();
		}

		/**
		 * @return the username
		 */
		public String getUsername() {
			return username.getValue();
		}

		/**
		 * @return the email
		 */
		public String getEmail() {
			return email.getValue();
		}

		/**
		 * @return the password
		 */
		public String getPassword() {
			return password.getValue();
		}

		/**
		 * @return the signupDate
		 */
		public Date getSignupDate() {
			return signupDate.getValue();
		}

		/**
		 * @return the lastLoginDate
		 */
		public Date getLastLoginDate() {
			return lastLoginDate.getValue();
		}

		/**
		 * @param userID the userID to set
		 */
		public void setUserID(Long userID) {
			this.userID.setValue(userID);
		}

		/**
		 * @param userID the userID to set
		 */
		public void setUserID(Integer userID) {
			this.userID.setValue(userID);
		}

		/**
		 * @param username the username to set
		 */
		public void setUsername(String username) {
			this.username.setValue(username);
		}

		/**
		 * @param email the email to set
		 */
		public void setEmail(String email) {
			this.email.setValue(email);
		}

		/**
		 * @param password the password to set
		 */
		public void setPassword(String password) {
			this.password.setValue(password);
		}

		/**
		 * @param signupDate the signupDate to set
		 */
		public void setSignupDate(Date signupDate) {
			this.signupDate.setValue(signupDate);
		}

		/**
		 * @param lastLoginDate the lastLoginDate to set
		 */
		public void setLastLoginDate(Date lastLoginDate) {
			this.lastLoginDate.setValue(lastLoginDate);
		}

		/**
		 * @return the blurb
		 */
		public String getBlurb() {
			return blurb.getValue();
		}

		/**
		 * @param blurb the blurb to set
		 */
		public void setBlurb(String blurb) {
			this.blurb.setValue(blurb);
		}

		/**
		 * @return a blurb
		 */
		public DBStringTrimmed queryBlurb() {
			return blurb;
		}

		/**
		 * @return the profileImageID
		 */
//		public DBInteger queryProfileImageID() {
//			return profileImageID;
//		}
		/**
		 * @return the profileImageID
		 */
//		public Long getProfileImageID() {
//			return profileImageID.getValue();
//		}
		/**
		 * @param profileImageID the profileImageID to set
		 */
//		public void setProfileImageID(Long profileImageID) {
//			this.profileImageID.setValue(profileImageID);
//		}
//	public boolean equals(User other) {
//		return (other == this)
//				|| (other.userID==this.userID)
//				|| Objects.equals(other.userID.getValue(), this.userID.getValue());
//	}
	}

}
