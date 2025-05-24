#ifndef UTILS_H
#define UTILS_H

#include "types.h"

void array_shift32(u32* array, const u16 start, const u16 end, const i16 direction);
void array_shift16(u16* array, const u16 start, const u16 end, const i16 direction);
void array_shift8(u8* array, const u16 start, const u16 end, const i16 direction);

void array_copy16(u16* from, u16* to, const u16 fromStart, const u16 toStart, const u16 amount);
void array_copy8(u8* from, u8* to, const u16 fromStart, const u16 toStart, const u16 amount);

#endif //UTILS_H
