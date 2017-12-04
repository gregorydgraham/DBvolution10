/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.mysql;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum MigrationFunctions {

	/**
	 *
	 */
	FINDFIRSTNUMBER("nvarchar(1000)", "sample nvarchar(1000)",
			"declare v_startpos int;\n"
			+ "declare endpos int;\n"
			+ "set v_startpos = nullif(least(\n"
			+ "case when locate('0', sample) > 0 then locate('0', sample) else length(sample)+1 end, \n"
			+ "case when locate('1', sample) > 0 then locate('1', sample) else length(sample)+1 end, \n"
			+ "case when locate('2', sample) > 0 then locate('2', sample) else length(sample)+1 end, \n"
			+ "case when locate('3', sample) > 0 then locate('3', sample) else length(sample)+1 end, \n"
			+ "case when locate('4', sample) > 0 then locate('4', sample) else length(sample)+1 end, \n"
			+ "case when locate('5', sample) > 0 then locate('5', sample) else length(sample)+1 end, \n"
			+ "case when locate('6', sample) > 0 then locate('6', sample) else length(sample)+1 end, \n"
			+ "case when locate('7', sample) > 0 then locate('7', sample) else length(sample)+1 end, \n"
			+ "case when locate('8', sample) > 0 then locate('8', sample) else length(sample)+1 end, \n"
			+ "case when locate('9', sample) > 0 then locate('9', sample) else length(sample)+1 end),length(sample)+1);\n"
			+ "if (v_startpos is null) then\n"
			+ " return null;\n"
			+ " else\n"
			+ "	set endpos = v_startpos;\n"
			+ "    while (endpos <= length(sample) and substring(sample,endpos,1) in ('0','1','2','3','4','5','6','7','8','9')) do\n"
			+ "		set endpos = endpos+1;\n"
			+ "    end while;\n"
			+ "    if(endpos <= length(sample) and substring(sample,endpos,1)='.')\n"
			+ "    then\n"
			+ "		if (endpos+1 <= length(sample) and substring(sample,endpos+1,1) in ('0','1','2','3','4','5','6','7','8','9'))\n"
			+ "        then\n"
			+ "			set endpos = endpos+1;\n"
			+ "			while (endpos <= length(sample) and substring(sample,endpos,1) in ('0','1','2','3','4','5','6','7','8','9')) do\n"
			+ "				set endpos = endpos+1;\n"
			+ "			end while;\n"
			+ "		end if;\n"
			+ "    end if;\n"
			+ "	if(v_startpos>1 and substring(sample,v_startpos-1,1) = '-') \n"
			+ "    then\n"
			+ "		set v_startpos = v_startpos-1;\n"
			+ "    end if;\n"
			+ "	return substring(sample, v_startpos,endpos - v_startpos);\n"
			+ "end if;\n"),
	FINDFIRSTINTEGER("nvarchar(1000)", "sample nvarchar(1000)",
			"declare v_startpos int;\n"
			+ "declare endpos int;\n"
			+ "set v_startpos = nullif(least(\n"
			+ "case when locate('0', sample) > 0 then locate('0', sample) else length(sample)+1 end, \n"
			+ "case when locate('1', sample) > 0 then locate('1', sample) else length(sample)+1 end, \n"
			+ "case when locate('2', sample) > 0 then locate('2', sample) else length(sample)+1 end, \n"
			+ "case when locate('3', sample) > 0 then locate('3', sample) else length(sample)+1 end, \n"
			+ "case when locate('4', sample) > 0 then locate('4', sample) else length(sample)+1 end, \n"
			+ "case when locate('5', sample) > 0 then locate('5', sample) else length(sample)+1 end, \n"
			+ "case when locate('6', sample) > 0 then locate('6', sample) else length(sample)+1 end, \n"
			+ "case when locate('7', sample) > 0 then locate('7', sample) else length(sample)+1 end, \n"
			+ "case when locate('8', sample) > 0 then locate('8', sample) else length(sample)+1 end, \n"
			+ "case when locate('9', sample) > 0 then locate('9', sample) else length(sample)+1 end),length(sample)+1);\n"
			+ "if (v_startpos is null) then\n"
			+ " return null;\n"
			+ " else\n"
			+ "	set endpos = v_startpos;\n"
			+ "    while (endpos <= length(sample) and substring(sample,endpos,1) in ('0','1','2','3','4','5','6','7','8','9')) do\n"
			+ "		set endpos = endpos+1;\n"
			+ "    end while;\n"
			+ "	if(v_startpos>1 and substring(sample,v_startpos-1,1) = '-') \n"
			+ "    then\n"
			+ "		set v_startpos = v_startpos-1;\n"
			+ "    end if;\n"
			+ "	return substring(sample, v_startpos,endpos - v_startpos);\n"
			+ "end if;\n");
	/**
	 *
	 */

	private final String returnType;
	private final String parameters;
	private final String code;

	MigrationFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_MIGRATION_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP FUNCTION " + this + ";");
		} catch (SQLException sqlex) {
			;
		}
		if (!this.code.isEmpty()) {
			final String createFn = "CREATE FUNCTION " + this + "(" + this.parameters + ")\n"
					+ "    RETURNS " + this.returnType
					+ "\n  BEGIN\n" + "\n" + this.code
					+ "\n END;";
			stmt.execute(createFn);
		}
	}

}
