#ifndef _UNDO_LOG_H_
#define _UNDO_LOG_H_

/**
 *
 * (iangneal): We need this for accurate indexing update performance numbers...
 * and of course, correctness.
 *
 * This will be the physicalized log (required for correctness) that will be used
 * during digest requests. It will log the following:
 *
 * 1) Block allocations/deallocations.
 * 2) Indexing structure modifications that are not atomic.
 * 3) [MAYBE] Inode allocations/deallocations (although this may be done by the
 *      LibFS side, I don't recall).
 *
 * The structure of the log should be as follows:
 *
 *  [DIGEST BEGIN]
 *  [UNDO ALLOCATION]
 *  [UNDO STRUCTURE GROWTH]
 *  ...
 *  [DIGEST END]
 *
 *  The size of each one of these entries should be a cacheline in size. Ergo,
 *  we can build an atomic log via TSX instructions.
 *
 *  For recovery, the protocol should be:
 *
 *  1) If [DIGEST END] is present, do nothing.
 *  2) If [DIGEST END] is NOT present, execute all undos, then replay from the 
 *      application log.
 *
 *
 * I'll use a third DAX device for this undo log.
 *
 */




#endif
