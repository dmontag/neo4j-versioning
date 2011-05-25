package org.neo4j.support.versioning.revision;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;

import java.util.Iterator;

public class VersionedNode implements Node
{
    private Node node;
    private VersionContext versionContext;

    public VersionedNode( Node node, VersionContext versionContext )
    {
        this.node = node;
        this.versionContext = versionContext;
    }

    public long getId()
    {
        return node.getId();
    }

    public void delete()
    {
        node.delete();
    }

    public Iterable<Relationship> getRelationships()
    {
        return getValidRelationships( node.getRelationships() );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return getValidRelationships( node.getRelationships( dir ) );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return getValidRelationships( node.getRelationships( types ) );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
    {
        return getValidRelationships( node.getRelationships( type, dir ) );
    }

    private FilteringIterable<Relationship> getValidRelationships( Iterable<Relationship> relationships )
    {
        return new FilteringIterable<Relationship>( relationships,
            new Predicate<Relationship>()
            {
                public boolean accept( Relationship item )
                {
                    return versionContext.hasRelValidVersion( node, item );
                }
            } );
    }

    public boolean hasRelationship()
    {
        return getRelationships().iterator().hasNext();
    }

    public boolean hasRelationship( Direction dir )
    {
        return getRelationships( dir ).iterator().hasNext();
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        return getRelationships( types ).iterator().hasNext();
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return getRelationships( type, dir ).iterator().hasNext();
    }

    public Relationship getSingleRelationship( RelationshipType type, Direction dir )
    {
        Iterator<Relationship> iter = getRelationships( type, dir ).iterator();
        if ( !iter.hasNext() )
        {
            return null;
        }
        Relationship single = iter.next();
        if ( iter.hasNext() ) throw new NotFoundException( "More than one relationship[" +
            type + ", " + dir + "] found for " + this + " in " + versionContext );
        return single;
    }

    public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
    {
        return node.createRelationshipTo( otherNode, type );
    }

    public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException( "Not implemented." );
    }

    public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection, RelationshipType secondRelationshipType, Direction secondDirection )
    {
        throw new UnsupportedOperationException( "Not implemented." );
    }

    public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections )
    {
        throw new UnsupportedOperationException( "Not implemented." );
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return node.getGraphDatabase();
    }

    public boolean hasProperty( String key )
    {
        return false;
    }

    public Object getProperty( String key )
    {
        return null;
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return null;
    }

    public void setProperty( String key, Object value )
    {
        if ( versionContext.shouldUseVersionedNodeProperties() )
        {
            org.neo4j.support.versioning.date.VersionContext.addVersionedProperty( node, key, value );
            return;
        }
        node.setProperty( key, value );
    }

    public Object removeProperty( String key )
    {
        if ( versionContext.shouldUseVersionedNodeProperties() )
        {
            return VersionContext.removeVersionedProperty( node, key );
        }
        return node.removeProperty( key );
    }

    public Iterable<String> getPropertyKeys()
    {
        return node.getPropertyKeys();
    }

    public Iterable<Object> getPropertyValues()
    {
        return node.getPropertyValues();
    }
}
