package org.directmemory.store;

import java.util.concurrent.LinkedBlockingQueue;

import org.directmemory.cache.CacheEntry;

public abstract class AbstractQueuedStore extends AbstractStore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private LinkedBlockingQueue<CacheEntry> queue = new LinkedBlockingQueue<CacheEntry>();
	public int queueSize = 100;
	
	private Thread popper = null; 

	@Override
	void popIn(CacheEntry entry) {
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
							CacheEntry queuedEntry = queue.poll();
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

	abstract void asyncPopIn(CacheEntry queuedEntry);
}
