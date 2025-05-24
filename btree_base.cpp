#include "types.h"
#include "btree_base.h"
#include "utils.h"

#define CLEANUP_FREE_CELLS 1
#define MINIMAL_CELL_SIZE 5

#define ENABLE_TRACE_CREATE_CURSOR 1
#define ENABLE_TRACE_CREATE_BTREE 0
#define ENABLE_TRACE_MERGE 1
#define ENABLE_TRACE_SPLIT 0
#define ENABLE_TRACE_INSERT_CELL 1
#define ENABLE_TRACE_DELETE_CELL 1
#define ENABLE_PRINT 1

#if ENABLE_TRACE || ENABLE_PRINT
#include <cstdio>
#endif

#if ENABLE_TRACE_CREATE_CURSOR
#	define TRACE_CREATE_CURSOR(x) TRACE(x)
#else
#	define TRACE_CREATE_CURSOR(x)
#endif

#if ENABLE_TRACE_CREATE_BTREE
#    define TRACE_CREATE_BTREE(x) TRACE(x)
#else
#    define TRACE_CREATE_BTREE(x)
#endif

#if ENABLE_TRACE_MERGE
#    define TRACE_MERGE(x) TRACE(x)
#else
#    define TRACE_MERGE(x)
#endif

#if ENABLE_TRACE_SPLIT
#    define TRACE_SPLIT(x) TRACE(x)
#else
#    define TRACE_SPLIT(x)
#endif

#if ENABLE_TRACE_INSERT_CELL
#    define TRACE_INSERT_CELL(x) TRACE(x)
#else
#    define TRACE_INSERT_CELL(x)
#endif

#if ENABLE_TRACE_DELETE_CELL
#    define TRACE_DELETE_CELL(x) TRACE(x)
#else
#    define TRACE_DELETE_CELL(x)
#endif

#define TRACE_PAGE_DATA(x) TRACE(("page %u, leaf %u, cPointers %u, cells %u\n", x->pageIndex, x->PageType, x->nCellPointersCount, x->nCellsTotalSize));

#if BTREE_LOCK_GRANULARITY_PER_PAGE
pthread_rwlock_t globalLock;
#endif

u64 decode(const u8 size, const u8 *pBegin) {
    u64 result = 0;
    for(i16 i = (i16)size - 1; i >= 0; --i) {
        result <<= 8;
        result |= pBegin[i];
    }
    return result;
}
u8 getValueByteSize(u64 value, u8 zeroWeight) {
    if (value == 0) {
        return zeroWeight;
    } else {
        for (u8 i = 1; i <= 8; ++i) {
            if (value < (256 << (8 * (i - 1)))) {
                return i;
            }
        }
    }
    return 8;
}
u8 encode(const u64 value, u8 *pBegin) {
    u8 valueByteSize = getValueByteSize(value, 0);
    u8 *pContent = pBegin;
    u64 curValue = value;

    while (curValue > 0) {
        *pContent = curValue & 0xFF;
        ++pContent;
        curValue >>= 8;
    }
    return valueByteSize;
}
u64 calculatePageRelevantSize(const Page* page, u1 includeHeader) {
    u64 result = page->nCellPointersCount * sizeof(u16) + (page->nCellsTotalSize - page->nFreeCellsTotalSize) * sizeof(u8);
    if (includeHeader)
        result += PAGER_PAGE_HEADER_SIZE;
    return result;
}
u64 calculatePageTotalSize(const Page* page, u1 includeHeader) {
    u64 result = page->nCellPointersCount * sizeof(u16) + page->nCellsTotalSize * sizeof(u8);
    if (includeHeader)
        result += PAGER_PAGE_HEADER_SIZE;
    return result;
}
void readPayload(const u8 *pCellStart, u64 *key, u64 *value) {
    u8 cellSize = *pCellStart;
    u8 headSize = *(pCellStart + 1);
    u8 payloadSize = *(pCellStart + 2);
    *key = decode(headSize, pCellStart + 3);
    *value = decode(payloadSize, pCellStart + 3 + headSize);
}
u8 writePayload(u8* pCellStart, const u64 key, u64 value, u8 forcedCellSize) {
    u8 keySize = getValueByteSize(key, 0);
    u8 valueSize = getValueByteSize(value, 0);

    u8 cellSize = forcedCellSize > 0 ? forcedCellSize : keySize + valueSize + 3;
    if (cellSize < MINIMAL_CELL_SIZE)
        cellSize = MINIMAL_CELL_SIZE;

    *pCellStart = cellSize;
    *(pCellStart + 1) = keySize;
    *(pCellStart + 2) = valueSize;
    encode(key, pCellStart + 3);
    encode(value, pCellStart + 3 + keySize);

    return cellSize;
}
void cleanCell(Page* page, u16 cellPointerIndex, u1 shiftPointers) {
    u16 cellSize = *(page->cells + page->cellPointers[cellPointerIndex]);
    u1 isLastCell = (page->cellPointers[cellPointerIndex] + cellSize == page->nCellsTotalSize);

    if (!isLastCell) {
        // mark cell as free, adding it to linked list of free cells
        u16 cellIndex = page->cellPointers[cellPointerIndex];
        u8 cellTotalSize = page->cells[cellIndex];
        if (cellTotalSize < MINIMAL_CELL_SIZE)
            cellTotalSize = MINIMAL_CELL_SIZE;
#if CLEANUP_FREE_CELLS
        for (u8 i = 0; i < cellTotalSize; ++i) {
            page->cells[cellIndex + i] = 0;
        }
#endif
        writePayload(page->cells + cellIndex, page->firstFreeCellIndex, 0, cellTotalSize);
        page->firstFreeCellIndex = cellIndex + 1;
        page->nFreeCellsTotalSize += cellTotalSize;
    } else {
        page->nCellsTotalSize -= *(page->cells + page->cellPointers[page->nCellPointersCount - 1]);
    }
    if(shiftPointers) {
        array_shift16(page->cellPointers, cellPointerIndex + 1, page->nCellPointersCount, -1);
        --page->nCellPointersCount;
    }
}

void BtreeCreateCursor(Btree* tree, Cursor** cursor, u1 write, u64 dbgI) {
    Cursor* cur = new Cursor;
#if BTREE_LOCK_GRANULARITY_EXCLUSIVE
    if(write) {
        TRACE_CREATE_CURSOR(("wrlock attt %llu\n", dbgI));
        pthread_rwlock_wrlock(&tree->lock);
        TRACE_CREATE_CURSOR(("wrlock took %llu\n", dbgI));
    } else {
        TRACE_CREATE_CURSOR(("rdlock attt %llu\n", dbgI));
        pthread_rwlock_rdlock(&tree->lock);
        TRACE_CREATE_CURSOR(("rdlock took %llu\n", dbgI));
    }
#endif

    cur->pRoot = tree->pRoot;
    cur->write = write;
    *cursor = cur;
}

void BtreeDestroyCursor(Btree* tree, Cursor* cursor, u64 dbgI) {
#if BTREE_LOCK_GRANULARITY_EXCLUSIVE
    TRACE_CREATE_CURSOR(("unlock %llu\n", dbgI));
    pthread_rwlock_unlock(&tree->lock);
#endif
    delete cursor;
}

u1 binarySearch(Page* page, const u64 key, u64 *resultValue, u16 *resultIndex) {
    u64 left = 0;
    u64 right = page->nCellPointersCount;
    u64 currentKey;
    u64 value;
    TRACE_PAGE_DATA(page);
    while(left < right) {
        u64 middle = (left + right) / 2;
        readPayload(page->cells + page->cellPointers[middle], &currentKey, &value);
        TRACE(("binSearch: curkey = %llu value = %llu\n", currentKey, value));
        if(currentKey == key) {
            TRACE(("binSearch: hit\n"));
            *resultValue = value;
            *resultIndex = middle;
            return 1;
        }

        if(currentKey < key) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    if(left < page->nCellPointersCount)
        readPayload(page->cells + page->cellPointers[left], &currentKey, &value);
    TRACE(("binSearch: left = %llu right = %llu\n", left, right));
    *resultValue = value;
    *resultIndex = left;
    return 0;
}
u8 BtreeCursorMoveTo(Cursor* cursor, const u64 key) {
    TRACE(("move invoked\n"));
    cursor->depth = 0;
    Page* page = cursor->pRoot;

    TRACE(("entering %u\n", cursor->pRoot->pageIndex));
    while (page->PageType != PAGER_PAGE_TYPE_LEAF) {
        cursor->pagePath[cursor->depth] = page->pageIndex;
        u64 k;
        u64 pageIndex;
        u16 pointerIndex;
        TRACE(("looking for %llu, depth = %d\n", key, cursor->depth));
        u1 moved = binarySearch(page, key, &pageIndex, &pointerIndex);
        TRACE(("found next pageIndex = %llu\n", pageIndex));

        if(cursor->depth == 1) {
            //BtreePrint(page);
        }

        if(pointerIndex >= page->nCellPointersCount)
            pointerIndex = page->nCellPointersCount - 1;

        //if(moved) {
            //cursor->pagePath[cursor->depth] = pageIndex;//page->cells;
            cursor->indices[cursor->depth++] = pointerIndex;
			pagerReleasePageLock(page);
            page = pagerGetReadPage(pageIndex);

        //} else {
//            cursor->pagePath[cursor->depth] = pageIndex;
//            cursor->indices[cursor->depth++] = pointerIndex - 1;
        //}

//        for (u16 i = 0; i < page->nCellPointersCount; ++i) {
//            readPayload(page->cells + page->cellPointers[i], &k, &pageIndex);
//            if (key <= k) {
//                cursor->indices[cursor->depth++] = i;
//                page = pagerGetPage(pageIndex);
//                moved = 1;
//                break;
//            }
//        }

//        if (!moved) {
//            cursor->indices[cursor->depth++] = page->nCellPointersCount - 1;
//            page = pagerGetPage(pageIndex);
//        }
    }
    TRACE(("leaving %u\n", cursor->depth));
    cursor->pagePath[cursor->depth] = page->pageIndex;

    u64 _;
    binarySearch(page, key, &_, cursor->indices + cursor->depth);
	pagerReleasePageLock(page);
//    u1 moved = 0;
//    for (u16 i = 0; i < page->nCellPointersCount; ++i) {
//        u64 k, v;
//        readPayload(page->cells + page->cellPointers[i], &k, &v);
//        if (k == key) {
//            cursor->indices[cursor->depth] = i;
//            moved = 1;
//            return CURSOR_MOVE_STATUS_FOUND;
//        }
//    }
//    if(!moved) {
//    }

    return 1;
}

void BtreeCursorFirstLeaf(Cursor* cursor) {
    cursor->depth = 0;
    Page* page = cursor->pRoot;
#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_rdlock(&page->lock);
#endif

    while (page->PageType != PAGER_PAGE_TYPE_LEAF) {
        u64 key;
        u64 pageIndex;
        cursor->indices[cursor->depth] = 0;
        cursor->pagePath[cursor->depth++] = page->pageIndex;
        readPayload(page->cells + page->cellPointers[0], &key, &pageIndex);
        pagerReleasePageLock(page);
        page = pagerGetReadPage(pageIndex);
    }
    pagerReleasePageLock(page);

    cursor->indices[cursor->depth] = 0;
    cursor->pagePath[cursor->depth] = page->pageIndex;
}

u1 BtreeCursorNextEntry(Cursor* cursor) {
    u8 d = cursor->depth;
    Page* node = pagerGetReadPage(cursor->pagePath[d]);
    u16 i = cursor->indices[d];

    if (i + 1 < node->nCellPointersCount) {
        // Next in current leaf
        cursor->indices[d]++;
        pagerReleasePageLock(node);
        return 1;
    }

    // Walk up to find a parent with a right sibling
    while (d > 0) {
        --d;
        Page* parent = pagerGetReadPage(cursor->pagePath[d]);
        u16 idx = cursor->indices[d];

        if (idx + 1 < parent->nCellPointersCount) {
            // Found a right sibling
            u64 k;
            u64 pageIndex;
            readPayload(parent->cells + parent->cellPointers[idx + 1], &k, &pageIndex);

            // Descend to leftmost leaf of this sibling
            Page* next = pagerGetReadPage(pageIndex);
            d++;
            while (next->PageType != PAGER_PAGE_TYPE_LEAF) {
                cursor->pagePath[d] = next->pageIndex;
                cursor->indices[d - 1]++;
                readPayload(next->cells + next->cellPointers[0], &k, &pageIndex);
                pagerReleasePageLock(next);
                next = pagerGetReadPage(pageIndex);
                d++;
            }
            pagerReleasePageLock(next);

            cursor->pagePath[d] = next->pageIndex;
            cursor->indices[d] = 0;
            cursor->depth = d;
            return 1;
        }
        pagerReleasePageLock(parent);
    }

    return 0;
}

u1 BtreeCursorReadData(const Cursor* cursor, u64 *key, u64 *value) {
    Page* page = pagerGetReadPage(cursor->pagePath[cursor->depth]);
    if (page->PageType != PAGER_PAGE_TYPE_LEAF) {
        return 1;
    }

    readPayload(page->cells + page->cellPointers[cursor->indices[cursor->depth]], key, value);
    pagerReleasePageLock(page);
    return 0;
}

void vacuumCells(Page* page) {
    u16 relevantCellPointers[page->nCellPointersCount];
    u8 relevantCells[page->nCellsTotalSize];
    u16 cellPointer = 0;
    for (u16 i = 0; i < page->nCellPointersCount; ++i) {
        relevantCellPointers[i] = cellPointer;
        u8 *cellStart = page->cells + page->cellPointers[i];
        u8 cellTotalSize = *cellStart;
        for (u8 *j = cellStart; j < cellStart + cellTotalSize; ++j) {
            relevantCells[cellPointer++] = *j;
        }
    }

    page->nCellsTotalSize = cellPointer;
    array_copy16(relevantCellPointers, page->cellPointers, 0, 0, page->nCellPointersCount);
    array_copy8(relevantCells, page->cells, 0, 0, cellPointer);
    page->nFreeCellsTotalSize = 0;
    page->firstFreeCellIndex = 0;
}

void insertCell(Cursor* cursor, const u8 depth, u64 key, u64 value, u1 newPointer);
void replaceKeyInParent(Cursor* cursor, const u8 depth, const u64 newKey) {
    if (depth == 0)
        return;
    //Page* parent = pagerGetPage(cursor->pagePath[depth - 1]);
    //u16 childIndex = cursor->indices[depth - 1];

    //u8* cellStart = parent->cells + parent->cellPointers[childIndex];
    insertCell(cursor, depth - 1, newKey, cursor->pagePath[depth], 0);
}
void splitNodes(Cursor* cursor, u8 depth) {
#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_rdlock(&globalLock);
#endif

    Page* current = pagerGetWritePage(cursor->pagePath[depth]);

    TRACE_SPLIT(("split started for depth %u", depth));

    Page* parent;
    Page* newLeft;
    Page* newRight;
    u16* cellPointers;
    u16 cellPointersCount;
    u8* cells;
    u16 cellsCount;

    if (depth == 0) {
        // parent stays on the same page, create 2 pages instead
        parent = current;
        newLeft = pagerCreateNewPage(current->PageType);
        newRight = pagerCreateNewPage(current->PageType);
#if BTREE_LOCK_GRANULARITY_PER_PAGE
        pthread_rwlock_wrlock(&newLeft->lock);
        pthread_rwlock_wrlock(&newRight->lock);
#endif
        vacuumCells(parent);
        cellPointers = parent->cellPointers;
        cellPointersCount = parent->nCellPointersCount;
        cells = parent->cells;
        cellsCount = parent->nCellsTotalSize;
    } else {
        parent = pagerGetWritePage(cursor->pagePath[depth - 1]);
        newLeft = current;
        newRight = pagerCreateNewPage(current->PageType);
#if BTREE_LOCK_GRANULARITY_PER_PAGE
        pthread_rwlock_wrlock(&newRight->lock);
#endif
        vacuumCells(current);
        cellPointers = current->cellPointers;
        cellPointersCount = current->nCellPointersCount;
        cells = current->cells;
        cellsCount = current->nCellsTotalSize;
    }

    TRACE_SPLIT(("splitNode: %u into %u + %u\n", current->pageIndex, newLeft->pageIndex, newRight->pageIndex));

    u16 midPtrIdx;
    u16 midCellIdx;
    u64 accumPageSize = 0;
    for (u16 i = 0; i < cellPointersCount; ++i) {
        accumPageSize += sizeof(u16) + *(cells + cellPointers[i]);
        if (accumPageSize >= PAGER_PAGE_BYTE_SIZE / 2) {
            midPtrIdx = i;
            midCellIdx = cellPointers[i];
            break;
        }
    }

    if (newLeft != current) {
        array_copy8(cells, newLeft->cells, 0, 0, midCellIdx);
        array_copy16(cellPointers, newLeft->cellPointers, 0, 0, midPtrIdx);
    }
    array_copy8(cells, newRight->cells, midCellIdx, 0, cellsCount - midCellIdx);
    for (u16 i = midPtrIdx; i < cellPointersCount; ++i) {
        newRight->cellPointers[i - midPtrIdx] = cellPointers[i] - midCellIdx;
    }

    newLeft->nCellsTotalSize = midCellIdx;
    newLeft->nCellPointersCount = midPtrIdx;
    newRight->nCellsTotalSize = cellsCount - midCellIdx;
    newRight->nCellPointersCount = cellPointersCount - midPtrIdx;

    u64 _;
    u64 leftMaxKey;
    u64 rightMaxKey;
    readPayload(newLeft->cells + newLeft->cellPointers[newLeft->nCellPointersCount - 1], &leftMaxKey, &_);
    readPayload(newRight->cells + newRight->cellPointers[newRight->nCellPointersCount - 1], &rightMaxKey, &_);

    u1 transferCursorRight = cursor->indices[depth] >= midPtrIdx;

    TRACE_SPLIT(("split medians cells = [ %u, %u ] pointers = [ %u %u ] max keys = [ %llu %llu ] \n", newLeft->nCellsTotalSize, newRight->nCellsTotalSize, newLeft->nCellPointersCount, newRight->nCellPointersCount, leftMaxKey, rightMaxKey));

    if (depth == 0) {
        parent->PageType = PAGER_PAGE_TYPE_PARENT;
        parent->nCellPointersCount = 0;
        parent->nCellsTotalSize = 0;
        parent->firstFreeCellIndex = 0;
        parent->nFreeCellsTotalSize = 0;

        // restore cursor position
        array_shift32(cursor->pagePath, 0, cursor->depth + 1, 1);
        array_shift16(cursor->indices, 0, cursor->depth + 1, 1);
        cursor->pagePath[0] = parent->pageIndex;
        ++cursor->depth;

        cursor->indices[0] = 0;
        cursor->pagePath[1] = newLeft->pageIndex;

        u8 prevDepth = cursor->depth;
        insertCell(cursor, depth, rightMaxKey, newRight->pageIndex, 1);
        depth += cursor->depth - prevDepth;
        prevDepth = cursor->depth;
        insertCell(cursor, depth, leftMaxKey, newLeft->pageIndex, 1);
        depth += cursor->depth - prevDepth;

        if (transferCursorRight) {
            cursor->indices[0] = 1;
            cursor->pagePath[1] = newRight->pageIndex;
            cursor->indices[1] -= midPtrIdx;
        }
    } else {
        // during replacement tree could grow, adjusting depth
        // pages remains the same, because node split affect only ancestors
        u8 prevDepth = cursor->depth;
        replaceKeyInParent(cursor, depth, leftMaxKey);
        depth += cursor->depth - prevDepth;

        ++cursor->indices[depth - 1];

        prevDepth = cursor->depth;
        insertCell(cursor, depth - 1, rightMaxKey, newRight->pageIndex, 1);
        depth += cursor->depth - prevDepth;

        if (transferCursorRight) {
            cursor->indices[depth] -= midPtrIdx;
            cursor->pagePath[depth] = newRight->pageIndex;
        } else {
            --cursor->indices[depth - 1];
        }
    }

    pagerReleasePageLock(newLeft);
    pagerReleasePageLock(newRight);
    pagerReleasePageLock(parent);

    TRACE_SPLIT(("split done %u\n", current->pageIndex));

}
void insertCell(Cursor* cursor, u8 depth, u64 key, u64 value, u1 newPointer) {
    Page* current = pagerGetWritePage(cursor->pagePath[depth]);

    u16 existingCellIndex = 0;
    u8 existingCellSize = 0;

    if(!newPointer) {
        existingCellIndex = current->cellPointers[cursor->indices[depth]];
        existingCellSize = *(current->cells + existingCellIndex);
    }

    u8 pointerSize = newPointer ? sizeof(u16) : 0;
    u8 expectedCellSize = 3 + getValueByteSize(key, 0) + getValueByteSize(value, 0);
    if (expectedCellSize < MINIMAL_CELL_SIZE)
        expectedCellSize = MINIMAL_CELL_SIZE;
    u8 actualCellSize = expectedCellSize;
    u1 payloadQuickWritten = 0;

    u64 prevMaxKey;
    u64 _;
    readPayload(current->cells + current->cellPointers[current->nCellPointersCount - 1], &prevMaxKey, &_);

    if(!newPointer) {
        if(existingCellSize >= expectedCellSize) {
            writePayload(current->cells + current->cellPointers[cursor->indices[depth]], key, value, existingCellSize);
            payloadQuickWritten = 1;
        } else {
            cleanCell(current, existingCellIndex, 0);
        }
    }

    if (!payloadQuickWritten) {
        u64 pageRelevantSize = calculatePageRelevantSize(current, 1);
        if (expectedCellSize + pointerSize + pageRelevantSize > PAGER_PAGE_BYTE_SIZE) {
            u8 prevDepth = cursor->depth;
            TRACE_INSERT_CELL(("insertCell: overflow detected for page %u, page pointers = %u, relevant size = %llu cell size = %u\n", current->pageIndex, current->nCellPointersCount, pageRelevantSize, expectedCellSize));
            splitNodes(cursor, depth);
            if (prevDepth != cursor->depth) {
                depth += cursor->depth - prevDepth;
                TRACE_INSERT_CELL(("insertCell: depth changed, retry\n"));
            }
            insertCell(cursor, depth, key, value, newPointer);
            //current = pagerGetPage(cursor->pagePath[depth]);
            pagerReleasePageLock(current);
            return;
        }

        // try to find a free cell to insert
        u16 freeCellIndex = current->firstFreeCellIndex;
        u16 prevCellIndex = 0;
        u8 prevCellSize = 0;
        while (freeCellIndex != 0) {
            u1 found = 0;
            u8 correctedIndex = freeCellIndex - 1;
            u8 freeCellSize = current->cells[correctedIndex];
            if (freeCellSize >= expectedCellSize) {
                actualCellSize = freeCellSize;
                found = 1;
            }
            u8 nextCellIndexSize = current->cells[correctedIndex + 1];

            u16 nextCellIndex = 0;
            freeCellIndex = 0;
            for (u8 i = nextCellIndexSize - 1; i >= 0; --i) {
                nextCellIndex <<= 8;
                nextCellIndex |= current->cells[correctedIndex + 2 + i];
            }

            if(found) {
            	// remove found free cell from the linked list
                if(prevCellIndex != 0) {
					writePayload(current->cells + prevCellIndex - 1, nextCellIndex, 0, prevCellSize);
                } else {
                	current->firstFreeCellIndex = nextCellIndex;
                }
                current->nFreeCellsTotalSize -= freeCellSize;
                break;
            } else {
                prevCellIndex = freeCellIndex;
                prevCellSize = freeCellSize;
				freeCellIndex = nextCellIndex;
            }
        }

        u16 insertionCellPointer = current->nCellsTotalSize;
        if (freeCellIndex != 0) {
            insertionCellPointer = freeCellIndex - 1;
        } else {
            u64 pageTotalSize = calculatePageTotalSize(current, 1);
            if (pageTotalSize + pointerSize + expectedCellSize > PAGER_PAGE_BYTE_SIZE) {
                vacuumCells(current);
                insertionCellPointer = current->nCellsTotalSize;
            }
        }

        if(newPointer)
            array_shift16(current->cellPointers, cursor->indices[depth], ++current->nCellPointersCount, 1);
        current->cellPointers[cursor->indices[depth]] = insertionCellPointer;
        writePayload(current->cells + insertionCellPointer, key, value, actualCellSize);
        TRACE_INSERT_CELL(("insertCell: success write in page %u for value %llu\n", current->pageIndex, key));
        if (freeCellIndex == 0)
            current->nCellsTotalSize += actualCellSize;
        pagerReleasePageLock(current);
    }

    u64 newMaxKey;
    readPayload(current->cells + current->cellPointers[current->nCellPointersCount - 1], &newMaxKey, &_);

    if(newMaxKey != prevMaxKey) {
        replaceKeyInParent(cursor, depth, key);
    }
}
void BtreeCursorInsertEntry(Btree *tree, Cursor* cursor, u64 key, u64 value) {
    if(!cursor->write)
        return;

#if 0
	pthread_rwlock_wrlock(&tree->lock);
#endif

    Page* current = pagerGetReadPage(cursor->pagePath[cursor->depth]);
    if (current->PageType != PAGER_PAGE_TYPE_LEAF)
        return;

    pagerReleasePageLock(current);

    insertCell(cursor, cursor->depth, key, value, 1);

#if 0
	pthread_rwlock_unlock(&tree->lock);
#endif
}

void removeCell(Cursor* cursor, const u8 depth, const u16 cellPointerIndex);
u1 mergeNodes(Cursor* cursor, u8 depth, PageIndex parentCellLeftIndex, PageIndex parentCellRightIndex) {
    Page* parent = nullptr;
    if (depth > 0)
        parent = pagerGetReadPage(cursor->pagePath[depth - 1]);
    else
        return 0;

    Page* left = pagerGetReadPage(parentCellLeftIndex);
    Page* right = pagerGetReadPage(parentCellRightIndex);

    TRACE_PAGE_DATA(left);
    TRACE_PAGE_DATA(right);

    u64 leftRelevantSize = calculatePageRelevantSize(left, 0); // left->nCellPointersCount * sizeof(u16) + (left->nCellsCount - left->nFreeCellsCount) * sizeof(u8);
    u64 rightRelevantSize = calculatePageRelevantSize(right, 0); //right->nCellPointersCount * sizeof(u16) + (right->nCellsCount - right->nFreeCellsCount) * sizeof(u8);
    if (PAGER_PAGE_HEADER_SIZE + leftRelevantSize + rightRelevantSize >= PAGER_PAGE_BYTE_SIZE) {
		pagerReleasePageLock(right);
    	pagerReleasePageLock(left);
    	pagerReleasePageLock(parent);
        TRACE_MERGE(("mergeNodes: size checking returned false, exiting merge\n"));
        return 0;
    } else {
    	TRACE_MERGE(("mergeNodes: size checking returned true\n"));
    }

    pagerGetWritePage(parentCellLeftIndex);
    pagerGetWritePage(parentCellRightIndex);
    pagerGetWritePage(cursor->pagePath[depth - 1]);


    vacuumCells(left);
    vacuumCells(right);

    array_copy8(right->cells, left->cells, 0, left->nCellsTotalSize, right->nCellsTotalSize);
    for (u16 i = 0; i < right->nCellPointersCount; ++i) {
        left->cellPointers[left->nCellPointersCount + i] = right->cellPointers[i] + left->nCellsTotalSize;
    }

    // shift cursor to the left if necessary
    if(cursor->pagePath[depth] == right->pageIndex) {
        TRACE_MERGE(("mergeNodes: shifting cursor to the left (depth = %u)\n", depth));
        cursor->pagePath[depth] = left->pageIndex;
        cursor->indices[depth] += left->nCellPointersCount;
        --cursor->indices[depth - 1];
    }

    left->nCellPointersCount = left->nCellPointersCount + right->nCellPointersCount;
    left->nCellsTotalSize = left->nCellsTotalSize + right->nCellsTotalSize;

	pagerReleasePageLock(right);
    pagerReleasePageLock(left);
    pagerReleasePageLock(parent);
    pagerFreePage(right->pageIndex);

    u16 rightPageCellPointerIndex = cursor->indices[depth - 1] + 1;
    u64 maxPageKey;
    u64 _;

    TRACE_PAGE_DATA(left);

    readPayload(left->cells + left->cellPointers[left->nCellPointersCount - 1], &maxPageKey, &_);
    TRACE_MERGE(("mergeNodes: new page max key = %llu\n", maxPageKey));
    removeCell(cursor, depth - 1, rightPageCellPointerIndex);
    TRACE_MERGE(("mergeNodes: replacing key in parent\n"));
    replaceKeyInParent(cursor, depth, maxPageKey);

    return 1;
}
void removeCell(Cursor* cursor, const u8 depth, const u16 cellPointerIndex) {
    Page* page = pagerGetWritePage(cursor->pagePath[depth]);

    TRACE_PAGE_DATA(page);

#if BTREE_LOCK_GRANULARITY_PER_PAGE
	pthread_rwlock_wrlock(&page->lock);
#endif

    u64 _;
    u64 keyForDelete;
	TRACE_DELETE_CELL(("removeCell: looking for cell #%u on page %u (depth = %u)\n", cellPointerIndex, cursor->pagePath[depth], depth));
    readPayload(page->cells + page->cellPointers[cellPointerIndex], &keyForDelete, &_);

    TRACE_DELETE_CELL(("removeCell: cleaning cell with found key %llu\n", keyForDelete));
    cleanCell(page, cellPointerIndex, 1);
    TRACE_PAGE_DATA(page);

    if (depth == 0) {
        TRACE_DELETE_CELL(("removeCell: done (depth = 0)\n"));
        return;
    }

    Page* parent = pagerGetWritePage(cursor->pagePath[depth - 1]);
    u16 idxInParent = cursor->indices[depth - 1];

    u64 keyInParent;
    readPayload(parent->cells + parent->cellPointers[idxInParent], &keyInParent, &_);
    if (keyInParent == keyForDelete) {
        if(page->nCellPointersCount == 0) {
            TRACE_DELETE_CELL(("removeCell: this was the last cell, clean parent\n"));
            removeCell(cursor, depth - 1, idxInParent);
            pagerFreePage(page->pageIndex);
            return;
        } else {
            TRACE_DELETE_CELL(("removeCell: need to replace key in parent\n"));
            u64 newMaxKey;
            readPayload(page->cells + page->cellPointers[page->nCellPointersCount - 1], &newMaxKey, &_);
            // lesser key never takes more bytes than current, so we guarantee it'll fit into cell
            replaceKeyInParent(cursor, depth, newMaxKey);
            TRACE_DELETE_CELL(("removeCell: parent key replaced with %llu\n", newMaxKey));
            //pagerFreePage(page->pageIndex);
        }
//        u8* cellStart = parent->cells + parent->cellPointers[idxInParent - 1];
//        if (idxInParent > 0)
//            readPayload(cellStart, &replacement, &_);
//
//        writePayload(cellStart, replacement, page->pageIndex, *cellStart);
    }

    u64 siblingIndex;
    u1 merged = 0;
    if (idxInParent < parent->nCellPointersCount - 1) {
        readPayload(parent->cells + parent->cellPointers[idxInParent + 1], &_, &siblingIndex);
        TRACE_DELETE_CELL(("removeCell: initialized merge with the right node\n"));
        merged = mergeNodes(cursor, depth, page->pageIndex, siblingIndex);
    }
    if (!merged && idxInParent > 0) {
        readPayload(parent->cells + parent->cellPointers[idxInParent - 1], &_, &siblingIndex);
        TRACE_DELETE_CELL(("removeCell: initialized merge with the left node\n"));
        mergeNodes(cursor, depth, siblingIndex, page->pageIndex);
    }

    TRACE_DELETE_CELL(("removeCell: done, %u\n", depth));
}
u1 BtreeCursorRemoveEntry(Cursor* cursor) {
    Page* page = pagerGetReadPage(cursor->pagePath[cursor->depth]);

    if (page->PageType != PAGER_PAGE_TYPE_LEAF || !cursor->write)
        return 0;

    pagerReleasePageLock(page);

    u16 entryIndex = cursor->indices[cursor->depth];

    removeCell(cursor, cursor->depth, entryIndex);
    return 1;
}

PageIndex createTreeCore(u64 *pKeys, u64 *pValues, u64 size, u1 isLeaf) {
    Page* current = nullptr;
    u64 totalPageSize = PAGER_PAGE_BYTE_SIZE;

    u16 pagesCount = 0;

    // assume all keys and values are 8-bytes long each, + 2 bytes for pointer + 3 bytes for header
    // divide this value by page content size, rounding up
    u64 pessimisticPageCount = (size * (sizeof(u64) * 2 + sizeof(u16) + 3) / (PAGER_PAGE_BYTE_SIZE - PAGER_PAGE_HEADER_SIZE)) + 2;
    TRACE_CREATE_BTREE(("pessimisticPageCount %llu for data %llu\n", pessimisticPageCount, size));

    u64 *pageKeys = new u64[pessimisticPageCount];
    u64 *pageIndices = new u64[pessimisticPageCount];

    for (u64 i = 0; i < size; ++i) {
        u64 keySize = getValueByteSize(pKeys[i], 0);
        u64 valueSize = getValueByteSize(pValues[i], 0);

        u8 expectedCellSize = keySize + valueSize + 3;
        if (expectedCellSize < MINIMAL_CELL_SIZE)
            expectedCellSize = MINIMAL_CELL_SIZE;

        totalPageSize += expectedCellSize + 2; // headers + cell pointer

        if (totalPageSize >= PAGER_PAGE_BYTE_SIZE) {
            TRACE_CREATE_BTREE(("allocating new Page %llu\n", totalPageSize));
            current = pagerCreateNewPage(isLeaf ? PAGER_PAGE_TYPE_LEAF : PAGER_PAGE_TYPE_PARENT);
            totalPageSize = PAGER_PAGE_HEADER_SIZE;
            current->nCellsTotalSize = 0;
            current->nCellPointersCount = 0;
            current->nFreeCellsTotalSize = 0;
            pageIndices[pagesCount++] = current->pageIndex;
        }

        current->cellPointers[current->nCellPointersCount++] = current->nCellsTotalSize;
        u8 totalCellSize = writePayload(current->cells + current->nCellsTotalSize, pKeys[i], pValues[i], 0);
        current->nCellsTotalSize += totalCellSize;

        pageKeys[pagesCount - 1] = pKeys[i];
    }
    if (pagesCount > 1) {
        TRACE_CREATE_BTREE(("creating new tree, pagesCount is %d\n", pagesCount));
        return createTreeCore(pageKeys, pageIndices, pagesCount, 0);
    }
    delete [] pageKeys;
    delete [] pageIndices;
    return current->pageIndex;
}
// builds tree from the array of given keys and values, keys must be ordered in ascending order
Btree* BtreeCreateTree(u64 *pKeys, u64 *pValues, u64 size) {
    PageIndex rootPageIdx = createTreeCore(pKeys, pValues, size, 1);
    Btree* tree = new Btree;
    tree->pRoot = pagerGetReadPage(rootPageIdx);
    pagerReleasePageLock(tree->pRoot);
    TRACE_CREATE_BTREE(("root page index %u\n", rootPageIdx));

#if BTREE_LOCK_GRANULARITY_EXCLUSIVE
    pthread_rwlock_init(&tree->lock, nullptr);
#endif

    return tree;
}

void BtreePrint(Page* root) {
#if ENABLE_PRINT
    printf("[ ");
    u64 key;
    u64 value;
    for (u16 i = 0; i < root->nCellPointersCount; ++i) {
        readPayload(root->cells + root->cellPointers[i], &key, &value);
        if (root->PageType == PAGER_PAGE_TYPE_LEAF) {
            printf("(%llu, %llu) ", key, value);
        } else {
            printf("%llu ", key);
            Page* child = pagerGetReadPage(value);
            BtreePrint(child);
            pagerReleasePageLock(child);
            printf(" ");
        }
    }
    printf("]");
#endif
}
