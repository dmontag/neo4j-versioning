package org.neo4j.support.versioning.date;

import org.junit.Test;
import org.neo4j.support.versioning.Range;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.support.versioning.Range.NIL;
import static org.neo4j.support.versioning.Range.asSet;
import static org.neo4j.support.versioning.Range.compactRanges;
import static org.neo4j.support.versioning.Range.range;

public class RangeTest
{
    @Test
    public void shouldGiveNilWhenRangesDontOverlapRegardlessOfOrder() throws Exception
    {
        assertEquals( NIL, range( 1, 4 ).intersect( range( 5, 8 ) ) );
        assertEquals( NIL, range( 5, 8 ).intersect( range( 1, 4 ) ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowExceptionWhenStartAfterEnd()
    {
        range( 2, 1 );
    }

    ////// intersection ///////

    @Test
    public void intersectShouldGiveEmptyRangeIfRangesJustTouchRegardlessOfOrder()
    {
        assertEquals( range( 3, 3 ), range( 1, 3 ).intersect( range( 3, 5 ) ) );
        assertEquals( range( 3, 3 ), range( 3, 5 ).intersect( range( 1, 3 ) ) );
    }

    @Test
    public void intersectShouldGiveIntersectionOfPartiallyOverlappingRegardlessOfOrder()
    {
        assertEquals( range( 5, 7 ), range( 1, 7 ).intersect( range( 5, 10 ) ) );
    }

    @Test
    public void intersectShouldGiveIntersectionOfFullyOverlappedDateRanges()
    {
        assertEquals( range( 5, 7 ), range( 1, 10 ).intersect( range( 5, 7 ) ) );
        assertEquals( range( 5, 7 ), range( 5, 7 ).intersect( range( 1, 10 ) ) );
    }

    @Test
    public void intersectShouldGiveIdentityForSameRange()
    {
        Range range = range( 1, 3 );
        assertEquals( range, range.intersect( range ) );
    }

    ////// union ///////

    @Test
    public void unionShouldGiveIdentityForSameRange()
    {
        Range range = range( 1, 3 );
        assertEquals( asSet( range ), range.union( range ) );
    }

    @Test
    public void unionShouldGiveSeparateRangesForDisjunctRanges()
    {
        assertEquals( asSet( range( 1, 3 ), range( 5, 7 ) ), range( 5, 7 ).union( range( 1, 3 ) ) );
    }

    @Test
    public void unionShouldGiveUnionOfOverlappingRanges()
    {
        assertEquals( asSet( range( 3, 7 ) ), range( 3, 6 ).union( range( 4, 7 ) ) );
        assertEquals( asSet( range( 3, 7 ) ), range( 4, 7 ).union( range( 3, 6 ) ) );
    }

    ////// compact //////

    @Test
    public void compactShouldHandleEmptySetOfRanges()
    {
        assertEquals( asList(), compactRanges( Arrays.<Range>asList() ) );
    }

    @Test
    public void compactShouldNotModifySingleRange()
    {
        List<Range> ranges = asList( range( 1, 2 ) );
        assertEquals( ranges, compactRanges( ranges ) );
    }

    @Test
    public void compactShouldMergeOverlappingRanges()
    {
        assertEquals( asList( range( 1, 3 ) ), compactRanges( asSet( range( 1, 2 ), range( 2, 3 ) ) ) );
        assertEquals( asList( range( 1, 4 ) ), compactRanges( asSet( range( 1, 2 ), range( 2, 3 ), range( 3, 4 ) ) ) );
        assertEquals( asList( range( 5, 12 ) ), compactRanges( asList( range( 7, 10 ), range( 5, 12 ) ) ) );
        assertEquals( asList( range( 1, 10 ), range( 15, 20 ) ), Range.compactRanges(
            asList( range( 1, 6 ), range( 4, 10 ), range( 15, 17 ), range( 19, 20 ), range( 17, 19 ) ) ) );
        assertEquals( asList( range( 1, 10 ) ), compactRanges( asList( range( 1, 4 ), range( 6, 10 ), range( 3, 7 ) ) ) );
    }

    @Test
    public void compactShouldNotMergeDisjunctRangesButIncludeBoth()
    {
        assertEquals( asList( range( 1, 2 ), range( 3, 4 ) ), compactRanges( asSet( range( 1, 2 ), range( 3, 4 ) ) ) );
        assertEquals( asList( range( 1, 2 ), range( 3, 4 ), range( 5, 6 ) ), compactRanges( asSet( range( 1, 2 ), range( 3, 4 ), range( 5, 6 ) ) ) );
    }

    @Test
    public void compactShouldMergeOverlappingDateRanges()
    {
    }

}
