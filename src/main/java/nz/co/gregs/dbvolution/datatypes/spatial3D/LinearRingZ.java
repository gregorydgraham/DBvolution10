/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

/**
 *
 * @author gregorygraham
 */
public class LinearRingZ extends LineStringZ {

	private static final long serialVersionUID = 1L;

	public LinearRingZ(CoordinateSequence coordinates, GeometryFactory factory) {
		super(coordinates, factory);
	}

	public LinearRingZ(Coordinate[] coordinates) {
		this(new GeometryFactory3D().getCoordinateSequenceFactory().create(coordinates), new GeometryFactory3D());
		
	}

	@Override
	public String toText() {
		WKTWriter writer = new WKTWriter(3);
		return writer.write(this);
	}

	@Override
	public String getGeometryType() {
		return "LinearRingZ";
	}
}
