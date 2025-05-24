#ifndef PAGER_H
#define PAGER_H

#include "types.h"
#if BTREE_LOCK_GRANULARITY_PER_PAGE
#include <pthread.h>
#endif

#define PAGER_PAGE_BYTE_SIZE 4096

#define PAGER_PAGE_TYPE_FREE 0
#define PAGER_PAGE_TYPE_LEAF 1
#define PAGER_PAGE_TYPE_PARENT 2

#define PAGER_PAGE_HEADER_SIZE 9

//const u16 PAGER_PAGE_HEADER_SIZE = sizeof(u8) + sizeof(u16) + sizeof(u16) + sizeof(u16) + sizeof(u16);

struct Page {
    u8 PageType;
    u16 nCellPointersCount;
    u16 nCellsTotalSize;
    u16 firstFreeCellIndex; // actually index + 1, 0 means no free cells
    u16 nFreeCellsTotalSize; // amount of free cells
    u16 cellPointers[(PAGER_PAGE_BYTE_SIZE - PAGER_PAGE_HEADER_SIZE) / sizeof(u16)];
    u8 cells[(PAGER_PAGE_BYTE_SIZE - PAGER_PAGE_HEADER_SIZE) / sizeof(u8)];
    PageIndex pageIndex;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
    pthread_rwlock_t lock;
#endif

    BTREE_MAYBE_PAGE_EXTRA_CONTENT
};
typedef struct Page Page;

void pagerInit(PageIndex totalPages);
Page* pagerCreateNewPage(u8 pageType);
void pagerFreePage(PageIndex pageIndex);

Page* pagerGetReadPage(PageIndex pageIndex);
Page* pagerGetWritePage(PageIndex pageIndex);
#if BTREE_LOCK_GRANULARITY_PER_PAGE
void pagerReleasePageLock(Page* page);
#else
#   define pagerReleasePageLock(x)
#endif

#endif //PAGER_H
