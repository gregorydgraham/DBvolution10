/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTWriter;

/**
 *
 * @author gregorygraham
 */
public class PointZ extends Point {
	
	private static final long serialVersionUID = 1L;	

	public PointZ(CoordinateSequence coordinates, GeometryFactory factory) {
		super(coordinates, factory);
	}

	@SuppressWarnings("deprecation")
	public PointZ(Coordinate coordinate, PrecisionModel precisionModel, int SRID) {
		super(coordinate, precisionModel, SRID);
	}

	@Override
	public String toText() {
		WKTWriter writer = new WKTWriter(3);
		return writer.write(this);
	}

	@Override
	public String getGeometryType() {
		return "PointZ";
	}

}
