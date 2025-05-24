#include "types.h"
#include "pager.h"

#define ENABLE_PAGER_TRACE 0
#if ENABLE_PAGER_TRACE
#	define PAGER_TRACE(x) TRACE(x)
#else
#	define PAGER_TRACE(x)
#endif

u1 inited = 0;
Page* Pages;
PageIndex FirstFreePageIndex;
PageIndex PageCount;
PageIndex ActivePages;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
pthread_rwlock_t pageAllocationLock;
#endif

void pagerInit(PageIndex totalPages) {
    if (inited) {
        delete [] Pages;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
		pthread_rwlock_destroy(&pageAllocationLock);
#endif
    }
    Pages = new Page[totalPages];
    FirstFreePageIndex = 0;
    PageCount = 0;
    ActivePages = 0;
    inited = 1;

#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_init(&pageAllocationLock, nullptr);
#endif
}

#if !BTREE_LOCK_GRANULARITY_PER_PAGE
Page* pagerGetPage(PageIndex pageIndex) {
	Page* result = Pages + pageIndex;
    return Pages + pageIndex;
}
#endif

Page* pagerCreateNewPage(u8 pageType) {
#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_wrlock(&pageAllocationLock);
#endif

    Page* newPage;
    if(FirstFreePageIndex == 0) {
        PAGER_TRACE(("pagerCreateNewPage: extend pages %u\n", PageCount));
        newPage = Pages + PageCount;
        newPage->pageIndex = PageCount;
        ++PageCount;
    } else {
        PAGER_TRACE(("pagerCreateNewPage: use free page %u\n", FirstFreePageIndex - 1));
        newPage = Pages + FirstFreePageIndex - 1;
        FirstFreePageIndex = newPage->cellPointers[0];
    }

    ++ActivePages;
    newPage->PageType = pageType;
    newPage->nCellsTotalSize = 0;
    newPage->firstFreeCellIndex = 0;
    newPage->nFreeCellsTotalSize = 0;

#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_init(&newPage->lock, nullptr);
	pthread_rwlock_unlock(&pageAllocationLock);
#endif

    return newPage;
}

void pagerFreePage(PageIndex pageIndex) {
#if BTREE_LOCK_GRANULARITY_PER_PAGE
    pthread_rwlock_wrlock(&pageAllocationLock);
#endif
    PAGER_TRACE(("pagerFreePage: dealloc page %u, ActivePages = %u\n", pageIndex, ActivePages - 1));

    Page* deallocatedPage = Pages + pageIndex;
    deallocatedPage->nCellPointersCount = 1;
    deallocatedPage->nCellsTotalSize = 0;
    deallocatedPage->cellPointers[0] = FirstFreePageIndex;
    deallocatedPage->PageType = PAGER_PAGE_TYPE_FREE;
    FirstFreePageIndex = pageIndex + 1;
    --ActivePages;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_destroy(&deallocatedPage->lock);
    pthread_rwlock_unlock(&pageAllocationLock);
#endif

}

Page* pagerGetReadPage(PageIndex pageIndex) {
    Page* result = Pages + pageIndex;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
    pthread_rwlock_rdlock(&result->lock);
#endif
	return result;
}

Page* pagerGetWritePage(PageIndex pageIndex) {
    Page* result = Pages + pageIndex;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
    pthread_rwlock_wrlock(&result->lock);
#endif
    return result;
}

#if BTREE_LOCK_GRANULARITY_PER_PAGE
void pagerReleasePageLock(Page* page) {
	pthread_rwlock_unlock(&page->lock);
}
#endif