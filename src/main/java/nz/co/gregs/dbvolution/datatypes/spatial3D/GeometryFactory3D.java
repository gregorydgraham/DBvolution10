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

/**
 *
 * @author gregorygraham
 */
public class GeometryFactory3D  extends GeometryFactory{

	private static final long serialVersionUID = 1L;

	public PolygonZ createPolygonZ(Coordinate[] coords){
		return createPolygonZ(createLinearRing(coords));
	}

	public PolygonZ createPolygonZ(LinearRing shell) {
		return createPolygonZ(shell, null);
	}

	public PolygonZ createPolygonZ(CoordinateSequence coordinates) {
		return createPolygonZ(createLinearRing(coordinates));
	}

	public PolygonZ createPolygonZ(LinearRing shell, LinearRing[] holes) {
		return new PolygonZ(shell, holes, this);
	} 

	public PolygonZ createPolygonZ(Polygon poly) {
		LinearRing shell = this.createLinearRing(poly.getExteriorRing().getCoordinates());
		LinearRing[] holes = null;
		final int numInteriorRing = poly.getNumInteriorRing();
		if (numInteriorRing>0){
			holes = new LinearRing[numInteriorRing];
			for(int i = 0; i<numInteriorRing;i++){
				LineString interiorRingN = poly.getInteriorRingN(i);
				holes[i] = this.createLinearRing(interiorRingN.getCoordinates());
			}
		}
		return createPolygonZ(shell, holes);
	}
	
	
	
}
