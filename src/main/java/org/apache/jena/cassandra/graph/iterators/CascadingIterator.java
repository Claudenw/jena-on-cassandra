package org.apache.jena.cassandra.graph.iterators;

import java.util.Iterator;


/**
 * An iterator that iterates over a base iterator and adds that result to a sub iterator.
 * This generates cross product between the two iterators.
 *
 * @param <T> The the base iterator iterates over.
 */
public abstract class CascadingIterator<T> implements Iterator<String> {
	/* the base iterator */
	private Iterator<T> baseIter;
	/**
	 * The current subIterator.  May be null.
	 */
	protected Iterator<String> subIter;
	/**
	 * The current value from the base iterator.
	 */
	protected T thisValue;
	/*
	 * if true this is the last in the cascading iterators.
	 */
	private boolean endIter = false;
	
	/**
	 * Constructor.
	 */
	public CascadingIterator() {
		
	}
	
	/**
	 * Method to set the base iterator.
	 * @param baseIter The base iterator to user.
	 */
	protected void setBaseIterator( Iterator<T> baseIter )
	{
		this.baseIter = baseIter;
	}
	
	/**
	 * Method to set the end iterator flag.  If set true then there is no
	 * sub iterator.  This must be set to true if there is not a sub iterator.
	 * @param state The end state.
	 */
	protected void setEndIter( boolean state )
	{
		this.endIter = state;
	}
	
	/**
	 * create the sub iterator.
	 * @return The next sub iterator.
	 */
	protected abstract Iterator<String> createSubIter();
	
	private boolean nextBase()
	{
		if (baseIter.hasNext())
		{
			thisValue = baseIter.next();
			subIter = null;
			if (endIter) {
				return true;
			}
			else
			{
				subIter = createSubIter();
				return subIter.hasNext();
			}
		} else
		{
			thisValue = null;
			return false;
		}
	}
	
	@Override
	public boolean hasNext() {
		if (thisValue == null)
		{
			return nextBase();
		} else {
			if (subIter == null || subIter.hasNext())
			{
				return true;
			}
			return nextBase();
		}
	}

}
