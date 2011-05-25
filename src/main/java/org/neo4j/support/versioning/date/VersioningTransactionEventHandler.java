package org.neo4j.support.versioning.date;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.support.versioning.Range;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.support.versioning.Range.range;
import static org.neo4j.support.versioning.date.VersionContext.*;

public class VersioningTransactionEventHandler implements TransactionEventHandler<Object>
{
    private final AtomicLong latestVersion = new AtomicLong( 0 );

    public Object beforeCommit( TransactionData data ) throws Exception
    {
        long version = latestVersion.incrementAndGet();
        System.out.println( "New version: " + version );
        for ( Node node : data.createdNodes() )
        {
            Range range = Range.range( version );
            setVersion( node, range );
            System.out.println( "* New node: " + node + ", setting version to " + range  );
        }
        for ( Relationship relationship : data.createdRelationships() )
        {
            Range range = range( version );
            setVersion( relationship, range );
            System.out.println( "* New relationship " + relationship + ", setting version to " + range );
        }

        for ( PropertyEntry<Relationship> relationshipPropertyEntry : data.assignedRelationshipProperties() )
        {
            if ( relationshipPropertyEntry.key().equals( VersionContext.DELETED_PROP_KEY ) )
            {
                Relationship rel = relationshipPropertyEntry.entity();
                System.out.println( "* Removed relationship: " + rel );
                setEndVersion( rel, version - 1 );
            }
        }

        Map<Node, Map<String, Object>> modifiedPropsByNode = new HashMap<Node, Map<String, Object>>();
        for ( PropertyEntry<Node> nodePropertyEntry : data.assignedNodeProperties() )
        {
            if ( nodePropertyEntry.key().equals( VersionContext.DELETED_PROP_KEY ) )
            {
                setEndVersion( nodePropertyEntry.entity(), version - 1 );
                continue;
            }
            if ( nodePropertyEntry.key().equals( VersionContext.VALID_FROM_PROPERTY )
                || nodePropertyEntry.key().equals( VersionContext.VALID_TO_PROPERTY ) )
            {
                continue;
            }
            Map<String, Object> modifiedProps = modifiedPropsByNode.get( nodePropertyEntry.entity() );
            if ( modifiedProps == null )
            {
                modifiedProps = new HashMap<String, Object>();
                modifiedPropsByNode.put( nodePropertyEntry.entity(), modifiedProps );
            }
            modifiedProps.put( nodePropertyEntry.key(), nodePropertyEntry.previouslyCommitedValue() );
        }
        for ( PropertyEntry<Node> nodePropertyEntry : data.removedNodeProperties() )
        {
            if ( nodePropertyEntry.key().equals( VersionContext.VALID_FROM_PROPERTY )
                || nodePropertyEntry.key().equals( VersionContext.VALID_TO_PROPERTY ) )
            {
                continue;
            }
            Map<String, Object> modifiedProps = modifiedPropsByNode.get( nodePropertyEntry.entity() );
            if ( modifiedProps == null )
            {
                modifiedProps = new HashMap<String, Object>();
                modifiedPropsByNode.put( nodePropertyEntry.entity(), modifiedProps );
            }
            modifiedProps.put( nodePropertyEntry.key(), nodePropertyEntry.previouslyCommitedValue() );
        }
        for ( Map.Entry<Node, Map<String, Object>> nodeEntry : modifiedPropsByNode.entrySet() )
        {
            Node mainNode = nodeEntry.getKey();
            Node newHistoricNode = mainNode.getGraphDatabase().createNode();
            copyProps( mainNode, newHistoricNode, nodeEntry.getValue() );
            insertFirstInChain( mainNode, newHistoricNode, version );
        }
        return null;
    }

    private static void copyProps( Node node, Node newNode, Map<String, Object> oldValues )
    {
        for ( String propKey : node.getPropertyKeys() )
        {
            newNode.setProperty( propKey, node.getProperty( propKey, null ) );
        }
        for ( Map.Entry<String, Object> propEntry : oldValues.entrySet() )
        {
            String key = propEntry.getKey();
            Object value = propEntry.getValue();
            if ( value == null )
            {
                newNode.removeProperty( key );
            }
            else
            {
                newNode.setProperty( key, value );
            }
        }
    }

    private static void insertFirstInChain( Node mainNode, Node newHistoricNode, long version )
    {
        Relationship prevVersionRel = mainNode.getSingleRelationship( VersionContext.PREV_VERSION_REL_TYPE, Direction.OUTGOING );
        if ( prevVersionRel != null )
        {
            newHistoricNode.createRelationshipTo( prevVersionRel.getOtherNode( mainNode ), VersionContext.PREV_VERSION_REL_TYPE );
            prevVersionRel.delete();
        }
        mainNode.createRelationshipTo( newHistoricNode, VersionContext.PREV_VERSION_REL_TYPE );
        setStartVersion( newHistoricNode, getStartVersion( mainNode ) );
        setEndVersion( newHistoricNode, version - 1 );
        setStartVersion( mainNode, version );
    }

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
