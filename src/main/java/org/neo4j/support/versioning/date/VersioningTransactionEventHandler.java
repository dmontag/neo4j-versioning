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

import static org.neo4j.support.versioning.Range.range;
import static org.neo4j.support.versioning.date.VersionContext.getStartVersion;
import static org.neo4j.support.versioning.date.VersionContext.setEndVersion;
import static org.neo4j.support.versioning.date.VersionContext.setStartVersion;
import static org.neo4j.support.versioning.date.VersionContext.setVersion;

public class VersioningTransactionEventHandler implements TransactionEventHandler<Object>
{
    private static final String LATEST_VERSION_PROP_KEY = "__LATEST_VERSION__";
    public static final String LOCK_PROP_KEY = "__DUMMY_LOCK__";
    private final Node versionDataNode;

    public VersioningTransactionEventHandler( Node versionDataNode )
    {
        this.versionDataNode = versionDataNode;
    }

    public Object beforeCommit( TransactionData data ) throws Exception
    {
        long version = getNextVersionNumber();
        processCreatedNodes( version, data.createdNodes() );
        processCreatedRelationships( version, data.createdRelationships() );
        processMarkedDeletedRelationships( version, data.assignedRelationshipProperties() );
        rotateProperties( version, findModifiedProperties( version, data ) );
        return null;
    }

    private long getNextVersionNumber()
    {
        versionDataNode.setProperty( LOCK_PROP_KEY, 0 );
        long latestVersion = getLatestVersion();
        long nextVersion = latestVersion + 1;
        setLatestVersion( nextVersion );
        return nextVersion;
    }

    private static void processCreatedNodes( long version, Iterable<Node> createdNodes )
    {
        for ( Node node : createdNodes )
        {
            Range range = Range.range( version );
            setVersion( node, range );
        }
    }

    private static void processCreatedRelationships( long version, Iterable<Relationship> createdRelationships )
    {
        for ( Relationship relationship : createdRelationships )
        {
            Range range = range( version );
            setVersion( relationship, range );
        }
    }

    private static void processMarkedDeletedRelationships( long version, Iterable<PropertyEntry<Relationship>> relationshipProperties )
    {
        for ( PropertyEntry<Relationship> relationshipPropertyEntry : relationshipProperties )
        {
            if ( relationshipPropertyEntry.key().equals( VersionContext.DELETED_PROP_KEY ) )
            {
                Relationship rel = relationshipPropertyEntry.entity();
                setEndVersion( rel, version - 1 );
            }
        }
    }

    private static Map<Node, Map<String, Object>> findModifiedProperties( long version, TransactionData data )
    {
        Map<Node, Map<String, Object>> modifiedPropsByNode = new HashMap<Node, Map<String, Object>>();
        for ( PropertyEntry<Node> nodePropertyEntry : data.assignedNodeProperties() )
        {
            if ( nodePropertyEntry.key().equals( VersionContext.DELETED_PROP_KEY ) )
            {
                setEndVersion( nodePropertyEntry.entity(), version - 1 );
                continue;
            }
            if ( isInternalProperty( nodePropertyEntry ) )
            {
                continue;
            }
            addEntryToMap( nodePropertyEntry, modifiedPropsByNode );
        }
        for ( PropertyEntry<Node> nodePropertyEntry : data.removedNodeProperties() )
        {
            if ( isInternalProperty( nodePropertyEntry ) )
            {
                continue;
            }
            addEntryToMap( nodePropertyEntry, modifiedPropsByNode );
        }
        return modifiedPropsByNode;
    }

    private static boolean isInternalProperty( PropertyEntry<Node> nodePropertyEntry )
    {
        return nodePropertyEntry.key().equals( VersionContext.VALID_FROM_PROPERTY )
            || nodePropertyEntry.key().equals( VersionContext.VALID_TO_PROPERTY );
    }

    private static void addEntryToMap( PropertyEntry<Node> nodePropertyEntry, Map<Node, Map<String, Object>> modifiedPropsByNode )
    {
        Map<String, Object> modifiedProps = modifiedPropsByNode.get( nodePropertyEntry.entity() );
        if ( modifiedProps == null )
        {
            modifiedProps = new HashMap<String, Object>();
            modifiedPropsByNode.put( nodePropertyEntry.entity(), modifiedProps );
        }
        modifiedProps.put( nodePropertyEntry.key(), nodePropertyEntry.previouslyCommitedValue() );
    }

    private static void rotateProperties( long version, Map<Node, Map<String, Object>> modifiedPropsByNode )
    {
        for ( Map.Entry<Node, Map<String, Object>> nodeEntry : modifiedPropsByNode.entrySet() )
        {
            Node mainNode = nodeEntry.getKey();
            Node newHistoricNode = mainNode.getGraphDatabase().createNode();
            copyProps( mainNode, newHistoricNode, nodeEntry.getValue() );
            insertFirstInChain( mainNode, newHistoricNode, version );
        }
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

    public void setLatestVersion( long version )
    {
        versionDataNode.setProperty( LATEST_VERSION_PROP_KEY, version );
    }

    public long getLatestVersion()
    {
        return (Long) versionDataNode.getProperty( LATEST_VERSION_PROP_KEY, 0L );
    }
}
