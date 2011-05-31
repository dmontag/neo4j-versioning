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
import org.neo4j.kernel.ImpermanentGraphDatabase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.support.versioning.date.VersionContext.vc;

public class VersionedGraphTest
{
    private ImpermanentGraphDatabase graphDb;
    private VersioningTransactionEventHandler versioningTransactionEventHandler;

    @Before
    public void setUp() throws IOException
    {
        graphDb = new ImpermanentGraphDatabase();
        versioningTransactionEventHandler = new VersioningTransactionEventHandler(graphDb.getReferenceNode());
        graphDb.registerTransactionEventHandler( versioningTransactionEventHandler );
    }

    @After
    public void tearDown()
    {
        graphDb.shutdown();
    }

    @Test
    public void testDiscoveryOfRelationships()
    {
        Node n1 = createNode();
        Node n2 = createNode();
        Relationship rel = createRelationship( n1, n2, RelTypes.LINKED );

        assertAdjacency( rel,
            vc( versioningTransactionEventHandler.getLatestVersion() ).node( n1 ),
            vc( versioningTransactionEventHandler.getLatestVersion() - 1 ).node( n1 ) );
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

        assertEquals( "foo", vc( fooVersion ).node( node ).getProperty( "key", null ) );
        assertEquals( "bar", vc( barVersion ).node( node ).getProperty( "key", null ) );
        assertEquals( "zoo", vc( zooVersion ).node( node ).getProperty( "key", null ) );
        assertEquals( null, vc( nokeyVersion ).node( node ).getProperty( "key", null ) );

        assertEquals( "foo", vc( fooVersion ).node( node ).getProperty( "key" ) );
        assertEquals( "bar", vc( barVersion ).node( node ).getProperty( "key" ) );
        assertEquals( "zoo", vc( zooVersion ).node( node ).getProperty( "key" ) );
        try
        {
            vc( nokeyVersion ).node( node ).getProperty( "key" );
            fail( "Should have thrown exception." );
        }
        catch ( NotFoundException e )
        {
        }

        assertTrue( vc( barVersion ).node( node ).hasProperty( "key" ) );
        assertFalse( vc( barVersion ).node( node ).hasProperty( "other" ) );
        assertTrue( vc( zooVersion ).node( node ).hasProperty( "other" ) );
        assertFalse( vc( nokeyVersion ).node( node ).hasProperty( "key" ) );
        assertTrue( vc( nokeyVersion ).node( node ).hasProperty( "other" ) );

        assertEquals( asSet( "key" ), addToSet( vc( barVersion ).node( node ).getPropertyKeys() ) );
        assertEquals( asSet( "key", "other" ), addToSet( vc( zooVersion ).node( node ).getPropertyKeys() ) );
        assertEquals( asSet( "other" ), addToSet( vc( nokeyVersion ).node( node ).getPropertyKeys() ) );

        assertEquals( asSet( "bar" ), addToSet( vc( barVersion ).node( node ).getPropertyValues() ) );
        assertEquals( asSet( "zoo", "asdf" ), addToSet( vc( zooVersion ).node( node ).getPropertyValues() ) );
        assertEquals( asSet( "asdf" ), addToSet( vc( nokeyVersion ).node( node ).getPropertyValues() ) );
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

        Relationship r1 = vc( firstVersion ).node( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n2, r1.getEndNode() );
        Relationship r2 = vc( secondVersion ).node( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
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

        Relationship r1 = vc( firstVersion ).node( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n2, r1.getEndNode() );
        Relationship r2 = vc( secondVersion ).node( n1 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( null, r2 );
        try
        {
            vc( secondVersion ).node( n2 );
            fail( "Should have thrown exception." );
        }
        catch ( NotFoundException e )
        {
        }
    }

    @Test
    public void testRelationshipMethodsThatReturnVersionedNodes() {
        Node n2 = createNode();
        Node n3 = createNode();

        createRelationship( n2, n3, RelTypes.LINKED );
        long snapshot = versioningTransactionEventHandler.getLatestVersion();
        createRelationship( n2, n3, RelTypes.LINKED );

        Relationship versionedRel = vc( snapshot ).node( n2 ).getSingleRelationship( RelTypes.LINKED, Direction.OUTGOING );
        assertEquals( n2, versionedRel.getStartNode() );
        assertEquals( n3, versionedRel.getEndNode() );
        assertEquals( n3, versionedRel.getOtherNode( n2 ) );
        assertEquals( n2, versionedRel.getOtherNode( n3 ) );
        assertTrue( versionedRel.getStartNode() instanceof VersionedNode );
        assertTrue( versionedRel.getEndNode() instanceof VersionedNode );
        assertTrue( versionedRel.getOtherNode( n2 ) instanceof VersionedNode );
        assertTrue( versionedRel.getOtherNode( n3 ) instanceof VersionedNode );
        Node[] nodes = versionedRel.getNodes();
        assertArrayEquals( new Node[] { n2, n3 }, nodes );
        assertTrue( nodes[0] instanceof VersionedNode );
        assertTrue( nodes[1] instanceof VersionedNode );
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
            vc( versioningTransactionEventHandler.getLatestVersion() ).deleteRelationship( relationship );
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
            vc( versioningTransactionEventHandler.getLatestVersion() ).deleteNode( node );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
