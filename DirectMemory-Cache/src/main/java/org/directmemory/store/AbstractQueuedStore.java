package org.directmemory.store;

import java.util.concurrent.LinkedBlockingQueue;

import org.directmemory.cache.CacheEntry2;

public abstract class AbstractQueuedStore extends ConcurrentAbstractStore implements Store {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected LinkedBlockingQueue<CacheEntry2> queue = new LinkedBlockingQueue<CacheEntry2>();
	public int queueSize = 100;
	
	final static String storeName = "abstract queued store";
	
	private Thread popper = null; 

	@Override
	void popIn(CacheEntry2 entry) {
		queue.add(entry);
		if (queue.size() >= queueSize && (popper == null || !popper.isAlive())) {
//			System.out.println("offheap popIn entry " + entry.key);
			if (popper == null) { 
				popper = new Thread(){
					public AbstractQueuedStore store;
					public Thread withStore(AbstractQueuedStore store) {
						this.store = store;
						return this;
					}
					public void run() {
//						System.out.println("we are in thread " + this);
						while (!queue.isEmpty()) {
							CacheEntry2 queuedEntry = queue.poll();
//							queuedEntry.getStore().toStream(queuedEntry);
//							queuedEntry.setStore(store);
							store.asyncPopIn(queuedEntry);
//							System.out.println("offheap popIn entry " + queuedEntry.key);
						}
//						System.out.println("we are exiting thread " + this);
					}
				}.withStore(this);
			} else {
				popper.run();
			}
		}
	}

	abstract void asyncPopIn(CacheEntry2 queuedEntry);
}
