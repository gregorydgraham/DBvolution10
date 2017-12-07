/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
@DBTableName(value = "spatialgen")
public class Spatialgen extends DBRow {

	private static final long serialVersionUID = 1L;
	@DBPrimaryKey
	@DBColumn
	@DBAutoIncrement
	public DBInteger pk_uid = new DBInteger();
	@DBColumn
	public DBPolygon2D poly = new DBPolygon2D();
	@DBColumn
	public DBPoint2D point = new DBPoint2D();
	@DBColumn
	public DBLine2D line = new DBLine2D();
	@DBColumn
	public DBMultiPoint2D mpoint = new DBMultiPoint2D();

}
