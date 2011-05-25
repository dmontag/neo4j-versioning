package org.neo4j.support.versioning.date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.support.versioning.ImpermanentGraphDatabase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.support.versioning.date.VersionContext.versionContext;

public class VersionedGraphTest
{
    private ImpermanentGraphDatabase graphDb;
    private VersioningTransactionEventHandler versioningTransactionEventHandler;

    @Before
    public void setUp() throws IOException
    {
        graphDb = new ImpermanentGraphDatabase();
        versioningTransactionEventHandler = new VersioningTransactionEventHandler();
        graphDb.registerTransactionEventHandler( versioningTransactionEventHandler );
    }

    @After
    public void tearDown()
    {
        graphDb.shutdown(false);
    }

    @Test
    public void testDiscoveryOfRelationships()
    {
        Node n1 = createNode();
        Node n2 = createNode();
        Relationship rel = createRelationship( n1, n2, RelTypes.LINKED );

        assertAdjacency( rel,
            versionContext( versioningTransactionEventHandler.getLatestVersion() ).forNode( n1 ),
            versionContext( versioningTransactionEventHandler.getLatestVersion() - 1 ).forNode( n1 ) );
    }

    @Test
    public void testVersionedProperties() {
        Node node = createNode();
        setProperty( node, "key", "foo" );
        long fooVersion = versioningTransactionEventHandler.getLatestVersion();
        setProperty( node, "key", "bar" );
        long barVersion = versioningTransactionEventHandler.getLatestVersion();
        setProperty( node, "key", "zoo" );
        setProperty( node, "other", "asdf" );
        long zooVersion = versioningTransactionEventHandler.getLatestVersion();
        removeProperty( node, "key" );
        long nokeyVersion = versioningTransactionEventHandler.getLatestVersion();

        assertEquals( "foo", versionContext( fooVersion ).forNode( node ).getProperty( "key", null ) );
        assertEquals( "bar", versionContext( barVersion ).forNode( node ).getProperty( "key", null ) );
        assertEquals( "zoo", versionContext( zooVersion ).forNode( node ).getProperty( "key", null ) );
        assertEquals( null, versionContext( nokeyVersion ).forNode( node ).getProperty( "key", null ) );

        assertEquals( "foo", versionContext( fooVersion ).forNode( node ).getProperty( "key" ) );
        assertEquals( "bar", versionContext( barVersion ).forNode( node ).getProperty( "key" ) );
        assertEquals( "zoo", versionContext( zooVersion ).forNode( node ).getProperty( "key" ) );
        try
        {
            versionContext( nokeyVersion ).forNode( node ).getProperty( "key" );
            fail( "Should have thrown exception." );
        }
        catch ( NotFoundException e )
        {
        }

        assertTrue( versionContext( barVersion ).forNode( node ).hasProperty( "key" ) );
        assertFalse( versionContext( barVersion ).forNode( node ).hasProperty( "other" ) );
        assertTrue( versionContext( zooVersion ).forNode( node ).hasProperty( "other" ) );
        assertFalse( versionContext( nokeyVersion ).forNode( node ).hasProperty( "key" ) );
        assertTrue( versionContext( nokeyVersion ).forNode( node ).hasProperty( "other" ) );

        assertEquals( asSet( "key" ), addToSet( versionContext( barVersion ).forNode( node ).getPropertyKeys() ) );
        assertEquals( asSet( "key", "other" ), addToSet( versionContext( zooVersion ).forNode( node ).getPropertyKeys() ) );
        assertEquals( asSet( "other" ), addToSet( versionContext( nokeyVersion ).forNode( node ).getPropertyKeys() ) );

        assertEquals( asSet( "bar" ), addToSet( versionContext( barVersion ).forNode( node ).getPropertyValues() ) );
        assertEquals( asSet( "zoo", "asdf" ), addToSet( versionContext( zooVersion ).forNode( node ).getPropertyValues() ) );
        assertEquals( asSet( "asdf" ), addToSet( versionContext( nokeyVersion ).forNode( node ).getPropertyValues() ) );
    }

    private <T> Set<T> asSet( T... t )
    {
        return new HashSet<T>( Arrays.asList( t ) );
    }

    @Test
    public void testRemovalOfRelationships()
    {

        Node n1 = createNode();
        Node n2 = createNode();
        Node n3 = createNode();
        Relationship firstRel = createRelationship( n1, n2, RelTypes.LINKED );
        long firstVersion = versioningTransactionEventHandler.getLatestVersion();
        removeRelationship( firstRel );
        createRelationship( n1, n3, RelTypes.LINKED );
        long secondVersion = versioningTransactionEventHandler.getLatestVersion();

        Relationship r1 = versionContext( firstVersion ).forNode( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n2, r1.getEndNode() );
        Relationship r2 = versionContext( secondVersion ).forNode( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n3, r2.getEndNode() );
    }

    @Test
    public void testRemovalOfNodes()
    {
        Node n2 = createNode();
        Node n1 = createNode();
        Relationship firstRel = createRelationship( n1, n2, RelTypes.LINKED );
        long firstVersion = versioningTransactionEventHandler.getLatestVersion();
        removeRelationship( firstRel );
        removeNode( n2 );
        long secondVersion = versioningTransactionEventHandler.getLatestVersion();

        Relationship r1 = versionContext( firstVersion ).forNode( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n2, r1.getEndNode() );
        Relationship r2 = versionContext( secondVersion ).forNode( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( null, r2 );
        try
        {
            versionContext( secondVersion ).forNode( n2 );
            fail( "Should have thrown exception." );
        }
        catch ( NotFoundException e )
        {
        }
    }

    private void assertAdjacency( Relationship rel, VersionedNode overlappingVersionedNode, VersionedNode nonOverlappingVersionedNode )
    {
        assertEquals( rel, overlappingVersionedNode.getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING ) );
        assertEquals( null, nonOverlappingVersionedNode.getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING ) );

        assertEquals( Collections.singleton( rel ),
            addToSet( overlappingVersionedNode.getRelationships() ) );
        assertEquals( Collections.EMPTY_SET,
            addToSet( nonOverlappingVersionedNode.getRelationships() ) );

        assertEquals( Collections.singleton( rel ),
            addToSet( overlappingVersionedNode.getRelationships( Direction.OUTGOING ) ) );
        assertEquals( Collections.EMPTY_SET,
            addToSet( nonOverlappingVersionedNode.getRelationships( Direction.OUTGOING ) ) );

        assertEquals( Collections.singleton( rel ),
            addToSet( overlappingVersionedNode.getRelationships( RelTypes.LINKED ) ) );
        assertEquals( Collections.EMPTY_SET,
            addToSet( nonOverlappingVersionedNode.getRelationships( RelTypes.LINKED ) ) );
    }

    private static <T> Set<T> addToSet( Iterable<T> iter )
    {
        return IteratorUtil.addToCollection( iter, new HashSet<T>() );
    }

    public Node createNode()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node node = graphDb.createNode();
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }



    enum RelTypes implements RelationshipType { LINKED;}
    private Relationship createRelationship( Node from, Node to, RelationshipType type )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Relationship rel = from.createRelationshipTo( to, type );
            tx.success();
            return rel;
        }
        finally
        {
            tx.finish();
        }
    }

    private void setProperty( Node node, String key, Object value )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            node.setProperty( key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void removeProperty( Node node, String key )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            node.removeProperty( key );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void removeRelationship( Relationship relationship )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            versionContext( versioningTransactionEventHandler.getLatestVersion() ).deleteRelationship( relationship );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void removeNode( Node node )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            versionContext( versioningTransactionEventHandler.getLatestVersion() ).deleteNode( node );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
