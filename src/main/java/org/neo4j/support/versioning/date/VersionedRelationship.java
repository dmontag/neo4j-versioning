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
        return relationship.getStartNode();
    }

    public Node getEndNode()
    {
        return relationship.getEndNode();
    }

    public Node getOtherNode( Node node )
    {
        return relationship.getOtherNode( node );
    }

    public Node[] getNodes()
    {
        return relationship.getNodes();
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
