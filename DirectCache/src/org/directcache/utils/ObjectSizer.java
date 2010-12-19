package org.directcache.utils;

/**
* Measures the approximate size of an object in memory, given a Class which
* has a no-argument constructor.
*/

@Deprecated
public final class ObjectSizer {

  /**
  * Return the approximate size in bytes, and return zero if the class
  * has no default constructor.
  *
  * @param aClass refers to a class which has a no-argument constructor.
  */
  @SuppressWarnings("unchecked")
public static long getObjectSize( @SuppressWarnings("rawtypes") Class aClass ){
    long result = 0;

    //if the class does not have a no-argument constructor, then
    //inform the user and return 0.
    try {
      aClass.getConstructor( new Class[]{} );
    }
    catch ( NoSuchMethodException ex ) {
      System.err.println(aClass + " does not have a no-argument constructor.");
      return result;
    }

    //this array will simply hold a bunch of references, such that
    //the objects cannot be garbage-collected
    Object[] objects = new Object[fSAMPLE_SIZE];

    //build a bunch of identical objects
    try {
      @SuppressWarnings("unused")
      Object throwAway = aClass.newInstance();

      long startMemoryUse = getMemoryUse();
      for (int idx=0; idx < objects.length ; ++idx) {
        objects[idx] = aClass.newInstance();
      }
      long endMemoryUse = getMemoryUse();

      float approximateSize = ( endMemoryUse - startMemoryUse ) /100f;
      result = Math.round( approximateSize );
    }
    catch (Exception ex) {
      System.err.println("Cannot create object using " + aClass);
    }
    return result;
  }

  // PRIVATE //
  private static int fSAMPLE_SIZE = 100;
  private static long fSLEEP_INTERVAL = 100;

  private static long getMemoryUse(){
    putOutTheGarbage();
    long totalMemory = Runtime.getRuntime().totalMemory();

    putOutTheGarbage();
    long freeMemory = Runtime.getRuntime().freeMemory();

    return (totalMemory - freeMemory);
  }

  private static void putOutTheGarbage() {
    collectGarbage();
    collectGarbage();
  }

  private static void collectGarbage() {
    try {
      System.gc();
      Thread.sleep(fSLEEP_INTERVAL);
      System.runFinalization();
      Thread.sleep(fSLEEP_INTERVAL);
    }
    catch (InterruptedException ex){
      ex.printStackTrace();
    }
  }
}