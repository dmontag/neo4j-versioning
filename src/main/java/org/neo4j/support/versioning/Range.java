/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.support.versioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Range implements Comparable<Range>
{
    public static final Range NIL = new Range( -1, -1 );

    private final long from;
    private final long to;

    public Range( long from, long to )
    {
        if ( from > to )
            throw new IllegalArgumentException( String.format( "From [%d] was after To [%d].", from, to ) );
        this.from = from;
        this.to = to;
    }

    public long from()
    {
        return from;
    }

    public long to()
    {
        return to;
    }

    public Range intersect( Range other )
    {
        if ( to < other.from || from > other.to ) return NIL;
        if ( to == other.from ) return new Range( to, to );
        if ( from == other.to ) return new Range( from, from );
        long newFrom = Math.max( from, other.from );
        long newTo = Math.min( to, other.to );
        return new Range( newFrom, newTo );
    }

    public Set<Range> union( Range other )
    {
        if ( intersect( other ) == NIL ) return asSet( this, other );
        if ( equals( other ) ) return asSet( this );
        return asSet( realUnion( other ) );
    }

    public static Set<Range> asSet( Range... items )
    {
        return new HashSet<Range>( Arrays.asList( items ) );
    }

    private Range realUnion( Range other )
    {
        long newFrom = Math.min( from, other.from );
        long newTo = Math.max( to, other.to );
        return new Range( newFrom, newTo );
    }

    public boolean overlaps( Range other )
    {
        return !intersect( other ).equals( NIL );
    }

    public boolean contains(long point) {
        return point >= from && point <= to;
    }

    @Override
    public String toString()
    {
        return String.format( "Range[%d,%d]", from, to );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        Range range = (Range) o;

        if ( from != range.from ) return false;
        if ( to != range.to ) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) ( from ^ ( from >>> 32 ) );
        result = 31 * result + (int) ( to ^ ( to >>> 32 ) );
        return result;
    }

    public int compareTo( Range o )
    {
        return Long.valueOf( from ).compareTo( o.from );
    }

    public static List<Range> compactRanges( Collection<Range> ranges )
    {
        List<Range> result = new ArrayList<Range>();
        takeNextRangeAndMergeOverlappingRanges( new ArrayList<Range>( ranges ), result );
        Collections.sort( result );
        return result;
    }

    private static void takeNextRangeAndMergeOverlappingRanges( List<Range> rangeList, List<Range> result )
    {
        if ( rangeList.isEmpty() ) return;
        result.add( consumeAndMergeOverlappingRanges( rangeList.remove( 0 ), rangeList ) );
        takeNextRangeAndMergeOverlappingRanges( rangeList, result );
    }

    private static Range consumeAndMergeOverlappingRanges( Range first, List<Range> rangeList )
    {
        for ( int i = 0; i < rangeList.size(); i++ )
        {
            Range rangeInList = rangeList.get( i );
            if ( first.overlaps( rangeInList ) )
            {
                first = first.union( rangeInList ).iterator().next();
                rangeList.remove( i );
                return consumeAndMergeOverlappingRanges( first, rangeList );
            }
        }
        return first;
    }

    public static Range range( long from, long to )
    {
        return new Range( from, to );
    }

    public static Range range( long from )
    {
        return new Range( from, Long.MAX_VALUE );
    }
}
