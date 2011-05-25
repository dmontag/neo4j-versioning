package org.neo4j.support.versioning.revision;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.support.versioning.ImpermanentGraphDatabase;
import org.neo4j.support.versioning.Range;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.neo4j.support.versioning.revision.VersionContext.*;

public class RevisionVersioningTest
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
        graphDb.shutdown();
    }

    @Test
    public void testVersionedRelationship()
    {
        Node n1 = createNode();
        Node n2 = createNode();
        Relationship rel = createRelationship( n1, n2, RelTypes.LINKED );
        assertAdjacency( rel,
            versionContext( versioningTransactionEventHandler.getLatestVersion() ).forNode( n1 ),
            versionContext( versioningTransactionEventHandler.getLatestVersion() - 1 ).forNode( n1 ) );

    }

    private void assertAdjacency( Relationship rel, Node overlappingVersionedNode, Node nonOverlappingVersionedNode )
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

    private static HashSet<Relationship> addToSet( Iterable<Relationship> relationships )
    {
        return IteratorUtil.addToCollection( relationships, new HashSet<Relationship>() );
    }

    public Node createNode( Range range )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node node = graphDb.createNode();
//            setVersion( node, range );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
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


    enum RelTypes implements RelationshipType
    {
        LINKED;
    }

    private Relationship createRelationship( Node from, Node to, RelationshipType type, Range range )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Relationship rel = from.createRelationshipTo( to, type );
//            setVersion( rel, range );
            tx.success();
            return rel;
        }
        finally
        {
            tx.finish();
        }
    }

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

}
