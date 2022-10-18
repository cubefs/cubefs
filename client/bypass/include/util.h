#ifndef UTIL_H
#define UTIL_H

#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static void log_debug(const char* message, ...) {
    va_list args;
    va_start(args, message);
    /*
    char *func = va_arg(args, char *);
    va_end(args);
    if(!strstr(func, "write")) return;
    va_start(args, message);
    */
    va_end(args);
    struct timeval now;
    gettimeofday(&now, NULL);
    struct tm *ptm = localtime(&now.tv_sec);
    char buf[27];
    strftime(buf, 20, "%F %H:%M:%S", ptm);
    sprintf(buf + 19, ".%.6d", now.tv_usec);
    buf[26] = '\0';
    fprintf(stderr, "%s [debug] ", buf);
    vprintf(message, args);
}

#endif
