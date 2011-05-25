package org.neo4j.support.versioning.revision;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class VersionContext
{

    public static final String VERSION_PROPERTY = "__version__";
    public static final RelationshipType PREV_VERSION_REL_TYPE = DynamicRelationshipType.withName( "__PREV_VERSION__" );
    private long version;
    private boolean useVersionedNodes;
    private boolean useVersionedNodeProperties;

    public static VersionContext versionContext( long version )
    {
        return new VersionContext( version );
    }

    public VersionContext( long version )
    {
        this.version = version;
    }

    public VersionedNode forNode( Node node )
    {
        return new VersionedNode( node, this );
    }


    public static void setVersion( PropertyContainer propertyContainer, long version )
    {
        propertyContainer.setProperty( VersionContext.VERSION_PROPERTY, version );
    }

    public static long getVersion( PropertyContainer propertyContainer )
    {
        Long version = (Long) propertyContainer.getProperty( VERSION_PROPERTY, null );
        return version != null ? version : -1;
    }

    public VersionContext useVersionedNodes()
    {
        useVersionedNodes = true;
        return this;
    }

    public boolean hasValidVersion( PropertyContainer propertyContainer )
    {
        long version = VersionContext.getVersion( propertyContainer );
//        return version != -1 && version.overlaps( range );
        return false;
    }

    public boolean hasRelValidVersion( Node node, Relationship relationship )
    {
        if ( useVersionedNodes && !hasValidVersion( relationship.getOtherNode( node ) ) ) return false;
        return hasValidVersion( relationship );

    }

    public VersionContext useVersionedNodeProperties()
    {
        useVersionedNodeProperties = true;
        return this;
    }

    public boolean shouldUseVersionedNodeProperties()
    {
        return useVersionedNodeProperties;
    }

    private static Node copyPropsToNewNode( Node node )
    {
        Node newNode = node.getGraphDatabase().createNode();
        for ( String propKey : node.getPropertyKeys() )
        {
            newNode.setProperty( propKey, node.getProperty( propKey, null ) );
        }
        return newNode;
    }

    private static void rotatePropertiesRelationships( Node node, Node newNode )
    {
//        Relationship olderPropsRel = node.getSingleRelationship( OLDER_PROPERTIES_REL_TYPE, Direction.OUTGOING );
//        if ( olderPropsRel != null )
//        {
//            newNode.createRelationshipTo( olderPropsRel.getOtherNode( node ), OLDER_PROPERTIES_REL_TYPE );
//            olderPropsRel.delete();
//        }
//        node.createRelationshipTo( newNode, OLDER_PROPERTIES_REL_TYPE );
    }

    public static void addVersionedProperty( Node node, String key, Object value )
    {
        Node newNode = copyPropsToNewNode( node );
        newNode.setProperty( key, value );
        rotatePropertiesRelationships( node, newNode );
    }

    public static Object removeVersionedProperty( Node node, String key )
    {
        Node newNode = copyPropsToNewNode( node );
        Object result = newNode.removeProperty( key );
        rotatePropertiesRelationships( node, newNode );
        return result;
    }
}
