/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTWriter;

/**
 *
 * @author gregorygraham
 */
public class PolygonZ extends Polygon {

	private static final long serialVersionUID = 1L;

//	static PolygonZ createPolygonZ(Polygon polygon) {
//		if (polygon != null && !Double.isNaN(polygon.getCoordinates()[0].z)) {
//			return (PolygonZ) polygon;
//		} else {
//			return null;
//		}
//	}
	public PolygonZ(LinearRing shell, LinearRing[] holes, GeometryFactory geomFactory) {
		super(shell, holes, geomFactory);
	}

	@Override
	public String toText() {
		WKTWriter writer = new WKTWriter(3);
		return writer.write(this);
	}

	@Override
	public String getGeometryType() {
		return "PolygonZ";
	}

	@Override
	public LineStringZ getExteriorRing() {
		final GeometryFactory3D fact = new GeometryFactory3D();
		return fact.createLineStringZ(super.getExteriorRing());
	}

	@Override
	public LineStringZ getInteriorRingN(int n) {
		return new GeometryFactory3D().createLineStringZ(holes[n]);
	}

}
