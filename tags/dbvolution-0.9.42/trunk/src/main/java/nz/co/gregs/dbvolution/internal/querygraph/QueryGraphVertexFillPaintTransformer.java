/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.internal.querygraph;

import java.awt.Color;
import java.awt.Paint;
import org.apache.commons.collections15.Transformer;

/**
 *
 * @author gregory.graham
 */
public class QueryGraphVertexFillPaintTransformer implements Transformer<QueryGraphNode, Paint> {

	public QueryGraphVertexFillPaintTransformer() {
	}

	@Override
	public Paint transform(QueryGraphNode i) {
		if (i.isRequiredNode()) {
			return Color.RED;
		} else {
			return Color.ORANGE;
		}
	}
	
}
