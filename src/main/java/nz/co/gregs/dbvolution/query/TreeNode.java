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

/**
 *
 * @author gregory.graham
 * @param <T>
 */
public class TreeNode<T> {

	private final List<TreeNode<T>> children = new ArrayList<TreeNode<T>>();
	private TreeNode<T> parent = null;
	private T data = null;

	public TreeNode(T data) {
		this.data = data;
	}

	public TreeNode(T data, TreeNode<T> parent) {
		this.data = data;
		this.parent = parent;
	}

	public List<TreeNode<T>> getChildren() {
		return children;
	}

	public void setParent(TreeNode<T> parent) {
		this.parent = parent;
	}

	public void addChild(T data) {
		TreeNode<T> child = new TreeNode<T>(data);
		child.setParent(this);
		this.children.add(child);
	}

	public void addChild(TreeNode<T> child) {
		child.setParent(this);
		this.children.add(child);
	}

	public T getData() {
		return this.data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public boolean isRoot() {
		return (this.parent == null);
	}

	public boolean isLeaf() {
		return this.children.isEmpty();
	}

	public void removeParent() {
		this.parent = null;
	}
}
