/*
 * Copyright (c) 2020 rxi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include "log.h"

#define MAX_NAME_LEN 256
#define MAX_FILE_SIZE 1073741824

typedef struct
{
  log_LogFn fn;
  void *udata;
  int level;
} Callback;

static struct
{
  void *udata;
  int level;
  bool quiet;
  FILE *fp;
  char filename[MAX_NAME_LEN];
  int64_t filesize;
  pthread_mutex_t lock;
} L;

static const char *level_strings[] = {
    "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

#ifdef LOG_USE_COLOR
static const char *level_colors[] = {
    "\x1b[94m", "\x1b[36m", "\x1b[32m", "\x1b[33m", "\x1b[31m", "\x1b[35m"};
#endif

static void stdout_callback(log_Event *ev)
{
  char tmp[64];
  tmp[strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", ev->time)] = '\0';
  char buf[74];
  snprintf(buf, sizeof(buf), "%s.%06ld", tmp, ev->time_usec);
#ifdef LOG_USE_COLOR
  fprintf(
      ev->udata, "%s %s%-5s\x1b[0m \x1b[90m%s:%d:\x1b[0m ",
      buf, level_colors[ev->level], level_strings[ev->level],
      ev->file, ev->line);
#else
  fprintf(
      ev->udata, "%s %-5s %s:%d: ",
      buf, level_strings[ev->level], ev->file, ev->line);
#endif
  vfprintf(ev->udata, ev->fmt, ev->ap);
  fprintf(ev->udata, "\n");
  fflush(ev->udata);
}

static void file_callback(log_Event *ev)
{
  int ret = 0;
  char tmp[64];
  tmp[strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", ev->time)] = '\0';
  char buf[74];
  int len = 0;
  snprintf(buf, sizeof(buf), "%s.%06ld", tmp, ev->time_usec);
  len = fprintf(
      ev->udata, "%s %-5s %s:%d: ",
      buf, level_strings[ev->level], ev->file, ev->line);
  ret = vfprintf(ev->udata, ev->fmt, ev->ap);
  fprintf(ev->udata, "\n");
  len = len + ret + 1;
  fflush(ev->udata);
  L.filesize += len;
}

const char *log_level_string(int level)
{
  return level_strings[level];
}

void log_set_level(int level)
{
  L.level = level;
}

void log_set_quiet(bool enable)
{
  L.quiet = enable;
}

static void init_event(log_Event *ev, void *udata)
{
  if (!ev->time)
  {
    // time_t t = time(NULL);
    // ev->time = localtime(&t);
    struct timeval tv;
    gettimeofday(&tv, NULL);
    ev->time = localtime(&tv.tv_sec);
    ev->time_usec = tv.tv_usec;
  }
  ev->udata = udata;
}

int log_set_filename(char *filename)
{
  struct stat buffer;
  int status;
  size_t len = MAX_NAME_LEN;

  pthread_mutex_init(&L.lock, NULL);

  if (len > strlen(filename))
  {
    len = strlen(filename);
  }
  strncpy(L.filename, filename, len);
  // open file and get file stats.
  L.fp = fopen(L.filename, "a");
  if (L.fp == NULL)
  {
    printf("open file(%s) failed\n", L.filename);
    return -1;
  }
  status = stat(L.filename, &buffer);
  if (status)
  {
    return -1;
  }
  L.filesize = buffer.st_size;

  return 0;
}

bool log_space_enough()
{
  struct statvfs buf;
  int ret;

  ret = statvfs(L.filename, &buf);
  if (ret)
  {
    return false;
  }

  if ((buf.f_blocks * 0.05) > buf.f_bavail)
  {
    return false;
  } else {
    return true;
  }
}

int log_get_and_del_old_file(char *dir_name)
{
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  int index = 0;
  char *pStart = NULL;
  char buf[MAX_NAME_LEN * 2];
  int ret = -1;

  dir = opendir(dir_name);
  if (dir == NULL)
  {
    return -1;
  }
  while ((entry = readdir(dir)) != NULL)
  {
    index = strlen(entry->d_name);
    if (strcmp(entry->d_name + index - 4, ".old") == 0)
    {
      strcpy(buf, dir_name);
      pStart = buf + strlen(dir_name);
      pStart[0] = '/';
      pStart++;
      strcpy(pStart, entry->d_name);
      remove(buf);
      ret = 0;
      goto out;
    }
  }

out:
  closedir(dir);
  return ret;
}

int log_check_disk_space()
{
  int i;
  int ret = 0;
  bool is_enough = true;
  char buf[MAX_NAME_LEN];
  char *dir_name = NULL;

  for (i = 0; i < 100; i++)
  {
    // check the disk space is larger than 5%.
    is_enough = log_space_enough();
    if (is_enough)
    {
      return 0;
    }
    // get the *.old file list.
    if (dir_name == NULL)
    {
      strncpy(buf, L.filename, strlen(L.filename));
      dir_name = dirname(buf);
    }
    // clean the old file until the disk space is larger than 5%.
    ret = log_get_and_del_old_file(dir_name);
    if (ret)
    {
      return ret;
    }
  }

  return 0;
}

int log_rotate_file(log_Event *ev)
{
  char tmp[64];
  char buf[MAX_NAME_LEN * 2];
  size_t len = 0;
  int status;
  int ret;

  // close the current log file.
  ret = fflush(L.fp);
  if (ret) {
    printf("failed to flush log file: %d\n", ret);
    return ret;
  }
  fclose(L.fp);
  L.fp = NULL;
  L.filesize = 0;

  // rename the current log file to *.old.
  memset(buf, 0, MAX_NAME_LEN * 2);
  strftime(tmp, sizeof(tmp), ".%Y%m%d%H%M%S.old", ev->time);
  sprintf(buf, "%s%s", L.filename, tmp);
  status = rename(L.filename, buf);
  if (status != 0)
  {
    printf("rename log failed\n");
    return -1;
  }

  // open a new log file.
  L.fp = fopen(L.filename, "a");
  if (L.fp == NULL)
  {
    printf("reopen log file failed\n");
    return -1;
  }

  return 0;
}

void log_log(int level, const char *file, int line, const char *fmt, ...)
{
  log_Event ev = {
      .fmt = fmt,
      .file = file,
      .line = line,
      .level = level,
  };
  int ret;

  if (L.quiet || level < L.level)
  {
    return;
  }

  pthread_mutex_lock(&L.lock);

  if (L.fp)
  {
    init_event(&ev, L.fp);
    va_start(ev.ap, fmt);
    file_callback(&ev);
    va_end(ev.ap);
  }
  else
  {
    init_event(&ev, stderr);
    va_start(ev.ap, fmt);
    stdout_callback(&ev);
    va_end(ev.ap);
  }

  if (L.fp && L.filesize >= MAX_FILE_SIZE)
  {
    // check the disk space is larger than 5%.
    ret = log_check_disk_space();
    if (ret)
    {
      goto unlock;
    }
    // rotate the log file.
    ret = log_rotate_file(&ev);
    if (ret)
    {
      goto unlock;
    }
  }

unlock:
  pthread_mutex_unlock(&L.lock);
}
