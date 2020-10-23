#ifndef _DEBUG_H_
#define _DEBUG_H_

#include <cstdio>
#include <cstring>

#ifdef DEBUG
#define DEBUG_ON 1
#else
#define DEBUG_ON 0
#endif

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PRINT_DEBUG(fmt, ...) \
    do { if (DEBUG_ON) fprintf(stderr, "[DEBUG:%s:%d]: " fmt, __FILENAME__, __LINE__, ##__VA_ARGS__); } while (0)

#endif /* _DEBUG_H_ */
