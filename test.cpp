#include <stdio.h>
#include "btree_base.h"
#include "pager.h"
#include <iostream>

using namespace std;

void test_delete() {
    pagerInit(10);

    const u16 dataSize = 15;
    u64 keys[dataSize];
    u64 values[dataSize];
    for (int i = 0; i < dataSize; i++) {
        keys[i] = i;
        values[i] = 10000 - i;
    }

    Btree* tree = BtreeCreateTree(keys, values, dataSize);

    Cursor* cursor;
    BtreeCreateCursor(tree, &cursor, 1);

    u64 key, value;

    for (int i = 0; i < 8; ++i) {
        BtreeCursorMoveTo(cursor, 11 * (i + 1));
        BtreeCursorRemoveEntry(cursor);
    }

    // BtreeCursorReadData(cursor, &key, &value);
    // printf("%llu %llu\n", key, value);
    //
    // BtreeCursorNextEntry(cursor);
    // BtreeCursorReadData(cursor, &key, &value);
    // printf("%llu %llu\n", key, value);
    //
    // BtreeCursorFirstLeaf(cursor);
    // BtreeCursorReadData(cursor, &key, &value);
    // printf("%llu %llu\n", key, value);
    //
    // BtreeCursorRemoveEntry(cursor);
    BtreeCursorFirstLeaf(cursor);
    BtreeCursorReadData(cursor, &key, &value);
    printf("%llu %llu\n", key, value);
}

void test_insert() {
    pagerInit(20000);
    const u64 dataSize = 1000000;
    u64* keys = new u64[dataSize];
    u64* values = new u64[dataSize];
    printf("array allocs");
    for (int i = 0; i < dataSize; i++) {
        keys[i] = i * 2;
        values[i] = 1000000 - i;
    }

    Btree* tree = BtreeCreateTree(keys, values, dataSize);

    Cursor* cursor;
    BtreeCreateCursor(tree, &cursor, 1);

    printf("BEFORE_INSERT\n");
    BtreePrint(tree->pRoot);
    printf("\n");

    for (int i = 0; i < dataSize; ++i) {
        BtreeCursorMoveTo(cursor, keys[i] + 1);
        BtreeCursorInsertEntry(cursor, keys[i] + 1, values[i]);
        // printf("inserted %llu\n", keys[i] + 1);
        // BtreePrint(root);
        // printf("\n");
    }
    printf("AFTER_INSERT\n");
    BtreePrint(tree->pRoot);
    printf("\n");

    for (int i = 0; i < dataSize - 1; ++i) {
        BtreeCursorMoveTo(cursor, keys[i] + 1);
        BtreeCursorRemoveEntry(cursor);
        BtreeCursorMoveTo(cursor, keys[i]);
        BtreeCursorRemoveEntry(cursor);

        // u64 expectedFirstLeaf = keys[i + 1];
        // BtreeCursorFirstLeaf(cursor);
        // u64 actualFirstLeaf, _;
        // BtreeCursorReadData(cursor, &actualFirstLeaf, &_);
        //
        // if (expectedFirstLeaf != actualFirstLeaf) {
        //     printf("MISMATCH expected: %llu actual: %llu\n", expectedFirstLeaf, actualFirstLeaf);
        //     break;
        // }
        // printf("removed %llu\n", keys[i]);
        // BtreePrint(root);
        // printf("\n");

    }
    //printf("removed %llu\n", keys[i]);
    BtreePrint(tree->pRoot);
    printf("\n");
}

void test_next_entry() {
    pagerInit(20000);
    const u64 dataSize = 1001;
    u64* keys = new u64[dataSize];
    u64* values = new u64[dataSize];
    printf("array allocs");
    for (u64 i = 0; i < dataSize; i++) {
        keys[i] = i + 1 ;//+ (i % 3);
        values[i] = i + 1 + (i % 3);
    }

    Btree *tree = BtreeCreateTree(keys, values, dataSize);

    Cursor* cursor;
    BtreeCreateCursor(tree, &cursor, 0);
    u64 __, ___;
    BtreeCursorFirstLeaf(cursor);
    for (u64 i = 0; i < dataSize; i++) {
        cout << "get entry " << i << endl;
        BtreeCursorNextEntry(cursor);
        BtreeCursorReadData(cursor, &__, &___);
    }


}

int main() {
    //test_insert();
    test_next_entry();
    return 0;
}