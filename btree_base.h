#ifndef BTREE_BASE_H
#define BTREE_BASE_H

#include "types.h"
#include "pager.h"

#include <pthread.h>

#define PAGER_MAX_TREE_DEPTH 8

#define CURSOR_MOVE_STATUS_FOUND 1
#define CURSOR_MOVE_STATUS_MISSED 2

struct Cursor {
    Page* pRoot;
    // array of page indices
    PageIndex pagePath[PAGER_MAX_TREE_DEPTH];
    // array of cell indices in pages
    u16 indices[PAGER_MAX_TREE_DEPTH];
    u8 depth;
    u1 write;
    struct Cursor* nextCursor;
};
typedef struct Cursor Cursor;

struct Btree {
    Page* pRoot;
    Cursor* firstCursor;
#if BTREE_LOCK_GRANULARITY_EXCLUSIVE
    pthread_rwlock_t lock;
#endif
};
typedef struct Btree Btree;

void BtreeCreateCursor(Btree* tree, Cursor** cursor, u1 write, u64 dbgI = 0);
void BtreeDestroyCursor(Btree* tree, Cursor* cursor, u64 dbgI = 0);
u8 BtreeCursorMoveTo(Cursor* cursor, u64 key);
void BtreeCursorFirstLeaf(Cursor* cursor);
u1 BtreeCursorNextEntry(Cursor* cursor);
u1 BtreeCursorReadData(const Cursor* cursor, u64 *key, u64 *value);
Btree *BtreeCreateTree(u64 *pKeys, u64 *pValues, u64 size);
void BtreeCursorInsertEntry(Btree *tree, Cursor* cursor, u64 key, u64 value);
u1 BtreeCursorRemoveEntry(Cursor *cursor);
void BtreePrint(Page *root);

#endif //BTREE_BASE_H
