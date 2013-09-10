/*
 * Copyright 2013 gregory.graham.
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
/**
 * Code generation from database schema, including updating existing
 * generated code when schemas change.
 * 
 * <p> Some implementation notes to be aware of when doing development within
 * this area:
 * <ul>
 * <li> The AST approach for code generation means that doing anything requires a lot
 * of lines of code to programmatically construct a source code statement. This includes
 * doing simple things such as including a piece of javadoc for a method, which
 * requires a lot of code to create each individual AST note type corresponding
 * to different parts of each line within a javadoc comment. On the face of it,
 * this seems like madness compared to just constructing the generated code as
 * a string. However, this extra effort for doing simple code generation is more than
 * made up for by how much easier it is to do the hard stuff, such as modifying
 * existing files and figuring out what's in an existing file.
 * <li> The Eclipse AST library was chosen because it's brilliant at making the really
 * hard stuff easy, and because it's guaranteed to be maintained for the foreseeable future.
 * <li> The Eclipse AST libraries are not available on any public maven repository,
 * so we have had to provide our own copy of it. Consequently we have to manually produce
 * a new version of those if we want to use an updated version of the Eclipse AST libraries.
 * </ul>
 */
package nz.co.gregs.dbvolution.generation;
