/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.support.versioning.date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class VersionedRelationship implements Relationship
{
    private Relationship relationship;
    private VersionContext versionContext;

    public VersionedRelationship( Relationship relationship, VersionContext versionContext )
    {
        this.relationship = relationship;
        this.versionContext = versionContext;
    }

    public long getId()
    {
        return relationship.getId();
    }

    public void delete()
    {
        versionContext.deleteRelationship( relationship );
    }

    public Node getStartNode()
    {
        return new VersionedNode( relationship.getStartNode(), versionContext );
    }

    public Node getEndNode()
    {
        return new VersionedNode( relationship.getEndNode(), versionContext );
    }

    public Node getOtherNode( Node node )
    {
        return new VersionedNode( relationship.getOtherNode( node ), versionContext );
    }

    public Node[] getNodes()
    {
        Node[] nodes = relationship.getNodes();
        return new Node[] { new VersionedNode( nodes[0], versionContext ), new VersionedNode( nodes[1], versionContext ) };
    }

    public RelationshipType getType()
    {
        return relationship.getType();
    }

    public boolean isType( RelationshipType type )
    {
        return relationship.isType( type );
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return relationship.getGraphDatabase();
    }

    public boolean hasProperty( String key )
    {
        return relationship.hasProperty( key );
    }

    public Object getProperty( String key )
    {
        return relationship.getProperty( key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return relationship.getProperty( key, defaultValue );
    }

    public void setProperty( String key, Object value )
    {
        relationship.setProperty( key, value );
    }

    public Object removeProperty( String key )
    {
        return relationship.removeProperty( key );
    }

    public Iterable<String> getPropertyKeys()
    {
        return relationship.getPropertyKeys();
    }

    public Iterable<Object> getPropertyValues()
    {
        return relationship.getPropertyValues();
    }

    @Override
    public int hashCode()
    {
        return relationship.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        return relationship.equals( obj );
    }
}
