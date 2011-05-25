package org.neo4j.support.versioning.revision;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class VersioningTransactionEventHandler implements TransactionEventHandler<Object>
{
    private final AtomicLong latestVersion = new AtomicLong( 0 );
    private static final RelationshipType PREV_VERSION_REL_TYPE = DynamicRelationshipType.withName( "__PREV_VERSION__" );

    public Object beforeCommit( TransactionData data ) throws Exception
    {
        long version = latestVersion.getAndIncrement();
        for ( Relationship relationship : data.createdRelationships() )
        {
            VersionContext.setVersion( relationship, version );
        }
        if ( IteratorUtil.count( data.assignedNodeProperties() ) == 0
            && IteratorUtil.count( data.removedNodeProperties() ) == 0)
        {
            return null;
        }
        Set<Node> nodesWithModifiedProps = new HashSet<Node>();
        for ( PropertyEntry<Node> nodePropertyEntry : data.assignedNodeProperties() )
        {
            nodesWithModifiedProps.add( nodePropertyEntry.entity() );
        }
        for ( PropertyEntry<Node> nodePropertyEntry : data.removedNodeProperties() )
        {
            nodesWithModifiedProps.add( nodePropertyEntry.entity() );
        }
        for ( Node node : nodesWithModifiedProps )
        {
            Node newNode = node.getGraphDatabase().createNode();
            copyProps( node, newNode );
            insertFirstInChain( node, newNode );
        }
        return null;
    }

    private static void copyProps( Node node, Node newNode )
    {
        for ( String propKey : node.getPropertyKeys() )
        {
            newNode.setProperty( propKey, node.getProperty( propKey, null ) );
        }
    }

    private static void insertFirstInChain( Node node, Node newNode )
    {
        Relationship prevVersionRel = node.getSingleRelationship( PREV_VERSION_REL_TYPE, Direction.OUTGOING );
        if ( prevVersionRel != null )
        {
            newNode.createRelationshipTo( prevVersionRel.getOtherNode( node ), PREV_VERSION_REL_TYPE );
            prevVersionRel.delete();
        }
        node.createRelationshipTo( newNode, PREV_VERSION_REL_TYPE );
    }

    /////////////////////////////////////////////////////////////////////////

    public void afterCommit( TransactionData data, Object state )
    {
    }

    public void afterRollback( TransactionData data, Object state )
    {
    }

    public void setLatestVersion( long latestVersion )
    {
        this.latestVersion.set( latestVersion );
    }

    public long getLatestVersion()
    {
        return latestVersion.get();
    }
}
