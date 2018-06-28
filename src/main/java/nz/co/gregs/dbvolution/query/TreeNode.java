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
package nz.co.gregs.dbvolution.query;

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Encapsulates a DBRow class for use in {@link DBRecursiveQuery} tree or path.
 *
 * <p>
 * Use {@link #getData() } to retrieve the DBRow contain in the node, {@link #getParent()
 * } to move up the hierarchy, and {@link #getChildren() } to move down it.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <T> The DBRow class
 */
public class TreeNode<T extends DBRow> {

	private final List<TreeNode<T>> children = new ArrayList<TreeNode<T>>();
	private final List<String> childrenKeys = new ArrayList<String>();
	private TreeNode<T> parent = null;
	private T data = null;

	/**
	 * Create a new node for the DBRow
	 *
	 * @param data
	 */
	public TreeNode(T data) {
		this.data = data;
	}

	/**
	 * Create a new node the contains the specified DBRow as the data, and the
	 * specified TreeNode as the parent of the node.
	 *
	 * @param data
	 * @param parent
	 */
	public TreeNode(T data, TreeNode<T> parent) {
		this.data = data;
		this.parent = parent;
	}

	/**
	 * Returns a list of all known children of this node, that is all database
	 * rows returned by the recursive query that referenced this row.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of TreeNodes
	 */
	public List<TreeNode<T>> getChildren() {
		return children;
	}

	private void setParent(TreeNode<T> parent) {
		this.parent = parent;
	}

	/**
	 * Append the supplied DBRow to the list of children for this node.
	 *
	 * <p>
	 * Also adds this node as the parent of the supplied node.
	 *
	 * @param childData
	 */
	public void addChild(T childData) {
		TreeNode<T> child = new TreeNode<T>(childData);
		this.addChild(child);
//		child.setParent(this);
//		if (notAlreadyIncluded(child)) {
//			this.children.add(child);
//			this.childrenData.add(data.getPrimaryKeys().stringValue());
//		}
	}

	/**
	 * Append the supplied DBRow to the list of children for this node.
	 *
	 * <p>
	 * Also adds this node as the parent of the supplied node.
	 *
	 * @param child
	 */
	public void addChild(TreeNode<T> child) {
		child.setParent(this);
		if (notAlreadyIncluded(child)) {
			this.children.add(child);
			this.childrenKeys.add(child.getKey());
		}
	}

	private boolean notAlreadyIncluded(TreeNode<T> child) {
		return !this.children.contains(child) && !childrenKeys.contains(child.getKey());
	}

	/**
	 * Retrieves the DBRow within this node.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a DBRow
	 */
	public T getData() {
		return this.data;
	}

	/**
	 * Set the DBRow within this node.
	 *
	 * @param data
	 */
	public void setData(T data) {
		this.data = data;
	}

	/**
	 * Indicates whether or not this node is a root node, that is it has no known
	 * parent.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this node is the top of a hierarchy.
	 */
	public boolean isRoot() {
		return (this.parent == null);
	}

	/**
	 * Indicates whether or not this node is a leaf node, that is it has no known
	 * children.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this node is the bottom of a hierarchy.
	 */
	public boolean isLeaf() {
		return this.children.isEmpty();
	}

	/**
	 * Remove the link to this node's parent, if any.
	 *
	 */
	public void removeParent() {
		this.parent = null;
	}

	/**
	 * Retrieves the node that is the parent of this node.
	 *
	 * <p>
	 * Use {@link #isRoot() } to determine if the root has been reached.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the TreeNode immediately above this node in the hierarchy
	 */
	public TreeNode<T> getParent() {
		return this.parent;
	}

	@Override
	public String toString() {
		return this.getData().toString();
	}

	private String getKey() {
		StringBuilder returnString = new StringBuilder("");
		final List<QueryableDatatype<?>> pks = this.getData().getPrimaryKeys();
		for (QueryableDatatype<?> pk : pks) {
			returnString.append("&&").append(pk.stringValue());
		}
		return returnString.toString();
	}
}
