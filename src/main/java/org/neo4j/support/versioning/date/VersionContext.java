package org.neo4j.support.versioning.date;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.support.versioning.Range;

public class VersionContext
{

    public static final String VALID_FROM_PROPERTY = "__valid_from__";
    public static final String VALID_TO_PROPERTY = "__valid_to__";
    public static final RelationshipType PREV_VERSION_REL_TYPE = DynamicRelationshipType.withName( "__PREV_VERSION__" );
    public static final String DELETED_PROP_KEY = "__deleted__";
    private long version;
    private boolean useVersionedNodes;

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
        getPropHolderNode( node );
        return new VersionedNode( node, this );
    }

    private static NotFoundException versionNotFound( long version )
    {
        return new NotFoundException( "Version [" + version + "] not found." );
    }

    public boolean hasValidVersion( PropertyContainer propertyContainer )
    {
        Range range = VersionContext.getVersion( propertyContainer );
        System.out.println("range: " + range);
        return range != null && range.contains( version );
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
        Relationship olderPropsRel = node.getSingleRelationship( PREV_VERSION_REL_TYPE, Direction.OUTGOING );
        if ( olderPropsRel != null )
        {
            newNode.createRelationshipTo( olderPropsRel.getOtherNode( node ), PREV_VERSION_REL_TYPE );
            olderPropsRel.delete();
        }
        node.createRelationshipTo( newNode, PREV_VERSION_REL_TYPE );
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

    public static void setVersion( PropertyContainer propertyContainer, Range range )
    {
        setStartVersion( propertyContainer, range.from() );
        setEndVersion( propertyContainer, range.to() );
    }

    public static void setStartVersion( PropertyContainer entity, long startVersion )
    {
        entity.setProperty( VALID_FROM_PROPERTY, startVersion );
    }

    public static void setEndVersion( PropertyContainer entity, long endVersion )
    {
        entity.setProperty( VALID_TO_PROPERTY, endVersion );
    }

    public static long getStartVersion( PropertyContainer entity )
    {
        return (Long) entity.getProperty( VALID_FROM_PROPERTY, -1L );
    }

    public static long getEndVersion( PropertyContainer entity )
    {
        return (Long) entity.getProperty( VALID_TO_PROPERTY, -1L );
    }

    public static Range getVersion( PropertyContainer propertyContainer )
    {
        Object from = propertyContainer.getProperty( VALID_FROM_PROPERTY, null );
        Object to = propertyContainer.getProperty( VALID_TO_PROPERTY, null );
        if ( from == null || to == null )
        {
            return null;
        }
        return new Range( (Long) from, (Long) to );
    }

    private static Node getPropHolderNodeForVersion( Node node, long version )
    {
        Range range = getVersion( node );
        System.out.println("Seeking prop holder for: " + node);
        if ( !range.contains( version ) )
        {
            Relationship prevVersionRel = node.getSingleRelationship( PREV_VERSION_REL_TYPE, Direction.OUTGOING );
            if ( prevVersionRel == null )
            {
                throw versionNotFound( version );
            }
            return getPropHolderNodeForVersion( prevVersionRel.getOtherNode( node ), version );
        }
        return node;
    }

    private Node getPropHolderNode( Node node )
    {
        return getPropHolderNodeForVersion( node, version );
    }

    public Object getProperty( Node node, String key )
    {
        return getPropHolderNode( node ).getProperty( key );
    }

    public boolean hasProperty( Node node, String key )
    {
        return getProperty( node, key, null ) != null;
    }

    public Object getProperty( Node node, String key, Object defaultValue )
    {
        try
        {
            return getProperty( node, key );
        }
        catch ( NotFoundException e )
        {
            return defaultValue;
        }
    }

    public Iterable<String> getPropertyKeys( Node node )
    {
        Node propHolderNode = getPropHolderNode( node );
        return rawGetPropertyKeys( propHolderNode );
    }

    public Iterable<Object> getPropertyValues( Node node )
    {
        final Node propHolderNode = getPropHolderNode( node );
        return new IterableWrapper<Object, String>( rawGetPropertyKeys( propHolderNode ) )
        {
            @Override
            protected Object underlyingObjectToObject( String object )
            {
                return propHolderNode.getProperty( object );
            }
        };
    }

    private Iterable<String> rawGetPropertyKeys( Node propHolderNode )
    {
        return new FilteringIterable<String>( propHolderNode.getPropertyKeys(), new Predicate<String>()
        {
            public boolean accept( String item )
            {
                return !item.equals( VALID_FROM_PROPERTY ) && !item.equals( VALID_TO_PROPERTY );
            }
        } );
    }

    public void deleteRelationship( Relationship relationship )
    {
        relationship.setProperty( DELETED_PROP_KEY, version );
    }

    public void deleteNode( Node node )
    {
        node.setProperty( DELETED_PROP_KEY, version );
    }
}
