#define USE_CUSTOM_COMPILED_BENCHMARK 0


#if USE_CUSTOM_COMPILED_BENCHMARK
#include "../benchmark/benchmark.h"
#else
#include <benchmark/benchmark.h>
#endif
#include "types.h"
#include "btree_base.h"
#include "pager.h"
#include <iostream>
#include <pthread.h>
#include <chrono>
#include <random>
#include <algorithm>

#define BENCHMARK_SHARED_SETTINGS \
    ->Unit(benchmark::kMillisecond) \
    ->ThreadRange(1, 8) \
	->MeasureProcessCPUTime() \
	->UseRealTime()

#define SINGLE_THREAD_PREPARATION(x) \
    if(state.thread_index() == 0) { \
        x \
        pthread_rwlock_init(&generatorLock, nullptr); \
        pthread_rwlock_init(&runningLock, nullptr); \
    } \
    u64 iterationIdx = 0; \
    u64 lastPreparationIdx = 0;

#define THREAD_PREPARE_ITERATION(preparationCode) \
    state.PauseTiming(); \
    ++iterationIdx; \
    if(lastPreparationIdx < iterationIdx) { \
        pthread_rwlock_wrlock(&generatorLock); \
        if (lastPreparationIdx < iterationIdx) { \
            pthread_rwlock_wrlock(&runningLock); \
            preparationCode; \
            lastPreparationIdx = iterationIdx; \
            pthread_rwlock_unlock(&runningLock); \
        } \
        pthread_rwlock_unlock(&generatorLock); \
    } \
    pthread_rwlock_rdlock(&runningLock); \
    state.ResumeTiming();

#define THREAD_COMPLETE_ITERATION(cleanupCode) \
    state.PauseTiming(); \
    pthread_rwlock_unlock(&runningLock); \
    cleanupCode \
    state.ResumeTiming();

#define SINGLE_THREAD_CLEANUP(x) \
    if(state.thread_index() == 0) { \
        x \
        pthread_rwlock_destroy(&generatorLock); \
        pthread_rwlock_destroy(&runningLock); \
    }

#define SEED 1832923

using namespace std;

static void generateData(u64 *keys, u64 *values, u64 dataSize, u64 keyDensity) {
    for (u64 i = 0; i < dataSize; ++i) {
        keys[i] = i * keyDensity;
        values[i] = i * i % 1524181;
    }
}

static int generateData2_weightedValue(mt19937_64 &rng) {
	u64 bytes = rng();
    bytes %= 100000;
    if(bytes < 4460) {
        bytes = 1;
    } else if (bytes < 44600 + 4460) {
        bytes = 2;
    } else if (bytes < 44600 * 2 + 4460) {
        bytes = 3;
    } else if (bytes < 44600 * 2 + 4460 * 2) {
        bytes = 4;
    } else if (bytes < 44600 * 2 + 4460 * 2 + 446) {
        bytes = 5;
    } else if (bytes < 44600 * 2 + 4460 * 2 + 446 * 2) {
        bytes = 6;
    } else if (bytes < 44600 * 2 + 4460 * 2 + 446 * 3) {
        bytes = 7;
    } else {
        bytes = 8;
    }
    u64 result = 0;
    for(u8 j = 0; j < bytes; ++j) {
		result <<= 8;
        u64 grow = rng() % 256;
        result += grow;
    }
    return result;
}

static void generateData2(u64 *keys, u64 *values, u64 dataSize, u64 keyDensity) {
    mt19937_64 rng;
    rng.seed(SEED);
	for (u64 i = 0; i < dataSize; ++i) {
		keys[i] = generateData2_weightedValue(rng);
        values[i] = generateData2_weightedValue(rng);
    }

}

Btree *seqWriteBtree;
u64 generatedBtreeVersion;
pthread_rwlock_t generatorLock, runningLock;
u64 *keys, *values;

static void BM_DbWorkload(benchmark::State& state) {
	u64 dataSize = state.range(0);
    SINGLE_THREAD_PREPARATION(
        keys = new u64[dataSize];
        values = new u64[dataSize];
        generateData2(keys, values, dataSize, 10);
        std::sort(keys, keys + dataSize);
    );

    u64 offset = state.thread_index() % 10 + 1;
    auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    u64 iters = 0;
    for (auto _ : state) {
        THREAD_PREPARE_ITERATION(
        	delete seqWriteBtree;
            pagerInit(100000);
            seqWriteBtree = BtreeCreateTree(keys, values, dataSize);
        )

        ++iters;
        Cursor* cursor;

        for(u64 i = 0; i < dataSize / 10; ++i) {
			u64 opCode = ((i + offset) * (i + offset) % 1000007) % 3;
        	u1 isWriteCursor = opCode != 0 ? 1 : 0;

        	BtreeCreateCursor(seqWriteBtree, &cursor, isWriteCursor, offset - 1);
        	u64 trueIdx = dataSize / 10 * offset + i;

            BtreeCursorMoveTo(cursor, keys[trueIdx]);

            TRACE(("opCode %llu", opCode));

            if(opCode == 1) {
                BtreeCursorInsertEntry(seqWriteBtree, cursor, keys[trueIdx] + offset, 42);
            } else if(opCode == 2) {
            	BtreeCursorRemoveEntry(cursor);
            }

            BtreeDestroyCursor(seqWriteBtree, cursor, offset - 1);
        }

        end = std::chrono::high_resolution_clock::now();
        THREAD_COMPLETE_ITERATION()
    }

    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    printf("TOTAL %f (%llu) %llu\n", elapsed_seconds.count(), offset, iters);

    SINGLE_THREAD_CLEANUP(
    	delete[] keys;
        delete[] values;
    )
}
BENCHMARK(BM_DbWorkload)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000L) // 1k - 10mln
    //->Iterations(10)
    BENCHMARK_SHARED_SETTINGS;

static void BM_RemoveOnly(benchmark::State& state) {
    u64 dataSize = state.range(0);
    SINGLE_THREAD_PREPARATION(
        keys = new u64[dataSize];
        values = new u64[dataSize];
        generateData(keys, values, dataSize, 10);
    );

    u64 offset = state.thread_index() % 10 + 1;
    auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    u64 iters = 0;
    for (auto _ : state) {
        THREAD_PREPARE_ITERATION(
        	delete seqWriteBtree;
            pagerInit(100000);
            seqWriteBtree = BtreeCreateTree(keys, values, dataSize);
        )

    	++iters;
        Cursor* cursor;
        BtreeCreateCursor(seqWriteBtree, &cursor, 1, offset - 1);

        for(u64 i = 0; i < dataSize / 10; ++i) {
            //printf("____________________________\n");
        	u64 trueIdx = dataSize / 10 * offset + i;
            if(trueIdx == 1260) {
            	//BtreePrint(seqWriteBtree->pRoot);
            }
            //printf("move to %llu %llu", trueIdx, offset);
            BtreeCursorMoveTo(cursor, keys[trueIdx]);
            //printf("remove entry %llu", offset);
            BtreeCursorRemoveEntry(cursor);
            //printf("done?\n");
        }

        //printf("destroy cursor\n");
        BtreeDestroyCursor(seqWriteBtree, cursor, offset - 1);

        end = std::chrono::high_resolution_clock::now();
        THREAD_COMPLETE_ITERATION()
    }

    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    printf("TOTAL %f (%llu) %llu\n", elapsed_seconds.count(), offset, iters);

    SINGLE_THREAD_CLEANUP(
    	delete[] keys;
        delete[] values;
    )
}
BENCHMARK(BM_RemoveOnly)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000L) // 1k - 10mln
    //->Iterations(10)
    BENCHMARK_SHARED_SETTINGS;

static void BM_InsertOnly(benchmark::State &state) {
    u64 dataSize = state.range(0);
    SINGLE_THREAD_PREPARATION(
        keys = new u64[dataSize];
        values = new u64[dataSize];
        generateData2(keys, values, dataSize, 10);
        std::sort(keys, keys + dataSize);
    )

    u64 offset = state.thread_index() % 10 + 1;

    auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    u64 iters = 0;
    for (auto _ : state) {
        THREAD_PREPARE_ITERATION(
            delete seqWriteBtree;
            pagerInit(100000);
            seqWriteBtree = BtreeCreateTree(keys, values, dataSize);
        )
        //auto start = std::chrono::high_resolution_clock::now();

        ++iters;
        Cursor* cursor;
        //printf("try%d", offset);
        BtreeCreateCursor(seqWriteBtree, &cursor, 1, offset - 1);
        //printf("%d\n", offset);

        // 10% modification
        for (u64 i = 0; i < dataSize / 10; i++) {
            u64 trueIdx = dataSize / 10 * offset + i;
            BtreeCursorMoveTo(cursor, keys[trueIdx]);
            BtreeCursorInsertEntry(seqWriteBtree, cursor, keys[trueIdx] + offset, 42);
        }

        //printf("rel%d\n", offset);
        BtreeDestroyCursor(seqWriteBtree, cursor, offset - 1);
        //auto end = std::chrono::high_resolution_clock::now();
        //auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        //state.SetIterationTime(elapsed_seconds.count());
        end = std::chrono::high_resolution_clock::now();

        THREAD_COMPLETE_ITERATION()
    }

    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
//    cout << "TOTAL: " << elapsed_seconds.count() << " (" << state.thread_index() << ") " << iters << endl;
    printf("TOTAL %f (%llu) %llu\n", elapsed_seconds.count(), offset - 1, iters);

    SINGLE_THREAD_CLEANUP(
    	delete[] keys;
    	delete[] values;
    )
}
BENCHMARK(BM_InsertOnly)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000L) // 1k - 10mln
    //->Iterations(10)
    BENCHMARK_SHARED_SETTINGS;

Btree *seqReadBtree;
static void BM_SequentialRead(benchmark::State &state) {
    u64 dataSize = state.range(0);
    SINGLE_THREAD_PREPARATION(
    	keys = new u64[dataSize];
    	values = new u64[dataSize];
        generateData2(keys, values, dataSize, 10);
        std::sort(keys, keys + dataSize);
        pagerInit(60000);
        seqReadBtree = BtreeCreateTree(keys, values, dataSize);
    )

    u64 idx = state.thread_index();
    u64 __, ___;


    auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    u64 iters = 0;
    for(auto _: state) {
		++iters;

        Cursor* cursor;
        BtreeCreateCursor(seqReadBtree, &cursor, 0, idx);
        BtreeCursorFirstLeaf(cursor);
        for (u64 i = 0; i < dataSize; i++) {
            //cout << "get entry " << i << endl;
            //BtreeCursorNextEntry(cursor);
            BtreeCursorMoveTo(cursor, keys[i]);
            BtreeCursorReadData(cursor, &__, &___);
        }
        BtreeDestroyCursor(seqReadBtree, cursor, idx);
        end = std::chrono::high_resolution_clock::now();
    }

    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
//    cout << "TOTAL: " << elapsed_seconds.count() << " (" << state.thread_index() << ") " << iters << endl;
    printf("TOTAL %f (%llu) %llu\n", elapsed_seconds.count(), idx, iters);

	SINGLE_THREAD_CLEANUP(
        delete seqReadBtree;
    	delete[] keys;
    	delete[] values;
	)
}
BENCHMARK(BM_SequentialRead)
    ->RangeMultiplier(10)
    ->Range(1000, 10000000L) // 1k - 10mln
    //->UseManualTime()
    //->Iterations(10)
    BENCHMARK_SHARED_SETTINGS;

BENCHMARK_MAIN();