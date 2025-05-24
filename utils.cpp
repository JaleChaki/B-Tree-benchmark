//
// Created by Alexander Syrtsev on 3/18/25.
//

#include "types.h"
#include "utils.h"

void array_shift32(u32* array, const u16 start, const u16 end, const i16 direction) {
    if(direction == 0)
        return;
    if(direction < 0) {
        for(u16 i = start; i < end; ++i) {
            array[i + direction] = array[i];
        }
    } else {
        for(u16 iOrig = end - 1, iShift = end - 1 + direction; iOrig >= start && iShift >= start + direction; --iOrig, --iShift) {
            array[iShift] = array[iOrig];
        }
    }
}

void array_shift16(u16* array, const u16 start, const u16 end, const i16 direction) {
    if(direction == 0)
        return;
    if(direction < 0) {
        for(u16 i = start; i < end; ++i) {
            array[i + direction] = array[i];
        }
    } else {
        for(u16 iOrig = end - 1, iShift = end - 1 + direction; iOrig >= start && iShift >= start + direction; --iOrig, --iShift) {
            array[iShift] = array[iOrig];
        }
    }
}

void array_shift8(u8* array, const u16 start, const u16 end, const i16 direction) {
    if(direction == 0)
        return;
    if(direction < 0) {
        for(u16 i = start; i < end; ++i) {
            array[i + direction] = array[i];
        }
    } else {
        for(u16 i = end - 1; i >= start; --i) {
            array[i] = array[i - direction];
        }
    }
}

void array_copy16(u16* from, u16* to, const u16 fromStart, const u16 toStart, const u16 amount) {
    for(u16 i = 0; i < amount; ++i) {
        to[toStart + i] = from[fromStart + i];
    }
}

void array_copy8(u8* from, u8* to, const u16 fromStart, const u16 toStart, const u16 amount) {
    for(u16 i = 0; i < amount; ++i) {
        to[toStart + i] = from[fromStart + i];
    }
}