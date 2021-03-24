/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */
/******************************************************************************\
*                                                                              *
*        Copyright (c) 2003, The Regents of the University of California       *
*      See the file COPYRIGHT for a complete copyright notice and license.     *
*                                                                              *
********************************************************************************
*
*  Implement abstract I/O interface for SIONlib.
*
\******************************************************************************/

/*
 * SION_CHECK will display a custom error message and then exit the program
 */

#define SION_CHECK(SION_RETURN, MSG) do {                              \
    char resultString[1024];                                             \
                                                                         \
    if (SION_RETURN < 0) {                                              \
        fprintf(stdout, "** error **\n");                                \
        fprintf(stdout, "ERROR in %s (line %d): %s.\n",                  \
                __FILE__, __LINE__, MSG);                                \
        fprintf(stdout, "ERROR: %d.\n", SION_RETURN)                 ;   \
        fprintf(stdout, "** exiting **\n");                              \
        exit(-1);                                                        \
    }                                                                    \
} while(0)

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "ior.h"
#include "iordef.h"
#include "aiori.h"
#include "utilities.h"
#include "sion.h"

/**************************** P R O T O T Y P E S *****************************/
static aiori_fd_t *SION_Create(char *, int iorflags, aiori_mod_opt_t *);
static aiori_fd_t *SION_Open(char *, int flags, aiori_mod_opt_t *);
static IOR_offset_t SION_Xfer(int, aiori_fd_t *, IOR_size_t *,
                                   IOR_offset_t, IOR_offset_t, aiori_mod_opt_t *);
static void SION_Close(aiori_fd_t *, aiori_mod_opt_t *);
static char* SION_GetVersion();
static void SION_Fsync(aiori_fd_t *, aiori_mod_opt_t *);
static int SION_check_params(aiori_mod_opt_t *);
static void SION_Delete(char *testFileName, aiori_mod_opt_t * module_options);
static IOR_offset_t SION_GetFileSize(aiori_mod_opt_t * module_options, char *testFileName);
static void SION_xfer_hints(aiori_xfer_hint_t * params);

static int sion_numfiles=1;//implement multiple files later

/************************** D E C L A R A T I O N S ***************************/
typedef struct {
  uint64_t delay_creates;
  uint64_t delay_xfer;
  int delay_rank_0_only;
} sion_options_t;

typedef struct {
  int filedesc;
}sion_fd_t;

static option_help * SION_options(aiori_mod_opt_t ** init_backend_options, aiori_mod_opt_t * init_values){
  sion_options_t * o = malloc(sizeof(sion_options_t));
  if (init_values != NULL){
    memcpy(o, init_values, sizeof(sion_options_t));
  }else{
    memset(o, 0, sizeof(sion_options_t));
  }

  *init_backend_options = (aiori_mod_opt_t*) o;

  option_help h [] = {
      {0, "sion.delay-create",        "Delay per create in usec", OPTION_OPTIONAL_ARGUMENT, 'l', & o->delay_creates},
      {0, "sion.delay-xfer",          "Delay per xfer in usec", OPTION_OPTIONAL_ARGUMENT, 'l', & o->delay_xfer},
      {0, "sion.delay-only-rank0",    "Delay only Rank0", OPTION_FLAG, 'd', & o->delay_rank_0_only},
      LAST_OPTION
  };
  option_help * help = malloc(sizeof(h));
  memcpy(help, h, sizeof(h));
  return help;
}

ior_aiori_t SION_aiori = {
        .name = "SION",
        .name_legacy = NULL,
        .create = SION_Create,
        .get_options = SION_options,
        .xfer_hints = SION_xfer_hints,
        .open = SION_Open,
        .xfer = SION_Xfer,
        .close = SION_Close,
        .delete = SION_Delete,
        .get_version = SION_GetVersion,
        .fsync = SION_Fsync, // ior.c validatetests allows you to remove certain possibilities
        .get_file_size = SION_GetFileSize,
        .statfs = aiori_posix_statfs,
        .mkdir = aiori_posix_mkdir,
        .rmdir = aiori_posix_rmdir,
        .stat = aiori_posix_stat,
        .check_params = SION_check_params,
};

static aiori_xfer_hint_t * hints = NULL;

static void SION_xfer_hints(aiori_xfer_hint_t * params){
  hints = params;
}
/************************** F U N C T I O N S ***************************/
static int SION_check_params(aiori_mod_opt_t * module_options){
  //dummy function to stop compiler warning.
  return 0;
}

static aiori_fd_t *SION_Create(char *testFileName, int iorflags, aiori_mod_opt_t * module_options)
{
  return SION_Open(testFileName, iorflags,module_options);
}

static aiori_fd_t *SION_Open(char *testFileName, int flags, aiori_mod_opt_t * module_options)
{
  MPI_Comm  gComm;
  MPI_Comm  lComm;
  int globalrank=rank;
  unsigned fd_mode = (unsigned)0;

  sion_mpi_options *options = sion_mpi_options_new();



  sion_mpi_options_set_chunksize(options,hints->blockSize);
  sion_mpi_options_set_fsblksize(options, -1);
  if(hints->filePerProc==0) {
    gComm=testComm;
  } else {
    gComm=MPI_COMM_SELF;
    globalrank=0;
  }


      sion_fd_t * sfd = malloc(sizeof(sion_fd_t));
      memset(sfd, 0, sizeof(sion_fd_t));
      MPI_Comm comm;
      MPI_Info mpiHints = MPI_INFO_NULL;

      if (flags & IOR_RDONLY) {
              fd_mode = SION_OPEN_READ;
      }
      if (flags & IOR_WRONLY) {
              fd_mode = SION_OPEN_WRITE;
      }
      if (flags & IOR_RDWR) {
              fprintf(stdout, "RDWR not implemented in SION\n");
      }
      if (flags & IOR_APPEND) {
              fprintf(stdout, "APPEND not implemented in SION\n");
      }
      if (flags & IOR_CREAT) {
              //Create is a flag but isn't needed for SION. Is there a create only option?
      }
      if (flags & IOR_RDWR) {
              fprintf(stdout, "RDWR not implemented in MPIIO\n");
      }

      if (hints->filePerProc) {
        comm=MPI_COMM_SELF;
      } else {
        comm = testComm;
      }




      if(! hints->dryRun){
          sfd->filedesc = sion_paropen_mpi(testFileName, fd_mode,comm, options);
          SION_CHECK(sfd->filedesc, "Cannot create file");

      }
      sion_mpi_options_delete(options);
      return ((void *) sfd);
}

static IOR_offset_t SION_Xfer(int access, aiori_fd_t *fdp, IOR_size_t * buffer,
                              IOR_offset_t length, IOR_offset_t offset, aiori_mod_opt_t * module_options)
{
  // Mostly old repurposed SION AIORI function, with 2.0 functions used instead.
  // To be reviewed

  int            xferRetries = 0;
  long long      remaining  = (long long)length;
  char         * ptr = (char *)buffer;
  long long      rc;
  // int            sid;
  IOR_offset_t   segmentPosition, segmentSize, segmentNum, PosInSegment;

  if (length != hints->transferSize) {
    char errMsg[256];
    sprintf(errMsg,"length(%lld) != hints->transferSize(%lld)\n",
	    length, hints->transferSize);
    SION_CHECK(-1, errMsg);
  }

  sion_fd_t * sfd= (sion_fd_t*) fdp;

  if (hints->filePerProc == TRUE) {
    segmentPosition = (IOR_offset_t)0;
    segmentSize = hints->blockSize;
    segmentNum = offset / segmentSize;
  } else {
    segmentSize     = (IOR_offset_t)(hints->numTasks) * hints->blockSize;
    segmentNum = offset / segmentSize;
    segmentPosition = (IOR_offset_t)((rank + rankOffset) % hints->numTasks) * hints->blockSize + segmentNum*segmentSize;
    PosInSegment    = (IOR_offset_t) offset - (IOR_offset_t) segmentPosition;
  }

  if (access == WRITE) { /* WRITE */
    if((offset-segmentPosition==0) && (segmentNum>0)) {
	sion_ensure_free_space(sfd->filedesc, (sion_int64) remaining);
      }
  }
  if ((access == READ)) { /* READ */
    if((offset-segmentPosition==0) && (segmentNum>0))  rc=sion_feof(sfd->filedesc);
  }

  /* mostly taken from POSIX API  */
  while (remaining > 0) {
    /* write/read file */
    if (access == WRITE) { /* WRITE */
      rc=sion_write(ptr,1,remaining,sfd->filedesc);
      /* rc = write(fd, ptr, remaining); */
      if (verbose >= VERBOSE_4) {
  printf("IOR_Xfer_SION[%03d]: wrote to sid=%d %lld bytes ... rc=%d\n",rank,sfd->filedesc,remaining,(int) rc);
      }
      /* if (hints->fsyncPerWrite == TRUE) IOR_Fsync_SION(&fd, hints); */
    } else {               /* READ or CHECK */
      rc=sion_read(ptr,1,remaining,sfd->filedesc);
      /* rc = read(fd, ptr, remaining); */
      if (verbose >= VERBOSE_4) {
  printf("IOR_Xfer_SION[%03d]: read from sid=%d %lld bytes ... rc=%d\n",rank,sfd->filedesc,remaining,(int) rc);
      }
      if (rc == 0) ERR("hit EOF prematurely");
    }
    if (rc == -1)      ERR("transfer failed");
    if (rc != remaining) {
      fprintf(stdout,  "WARNING: Task %d requested transfer of %lld bytes,\n", rank, remaining);
      fprintf(stdout,  "         but transferred %lld bytes at offset %lld\n", rc, offset + length - remaining);
      if (hints->singleXferAttempt == TRUE)
  MPI_CHECK(MPI_Abort(MPI_COMM_WORLD, -1), "barrier error");
    }
    if (rc < remaining) {
      if (xferRetries > MAX_RETRY)
  ERR("too many retries -- aborting");
      if (xferRetries == 0) {
  if (access == WRITE) {
    WARN("This file system requires support of partial write()s");
  } else {
    WARN("This file system requires support of partial read()s");
  }
  fprintf(stdout,
    "WARNING: Requested xfer of %lld bytes, but xferred %lld bytes\n",
    remaining, rc);
      }
      if (verbose >= VERBOSE_2) {
  fprintf(stdout, "Only transferred %lld of %lld bytes\n",rc, remaining);
      }
    }
    if (rc > remaining) /* this should never happen */
      ERR("too many bytes transferred!?!");
    remaining -= rc;
    ptr += rc;
    xferRetries++;
  }
  if (verbose >= VERBOSE_4) {
    printf("IOR_Xfer_SION[%03d]: %d %lld ready\n",rank,access,length);
  }
  return(length);
}

static void SION_Fsync(aiori_fd_t *fdp, aiori_mod_opt_t * module_options)
{
  //SION does not currently buffer, so does not need to do this?
  if (verbose >= VERBOSE_2) {
    printf("IOR_Fsync_SION[%03d]:\n",rank);
  }
  return;
}

static void SION_Close(aiori_fd_t *fdp, aiori_mod_opt_t * module_options)
{
  if (verbose >= VERBOSE_2) {
    printf("IOR_Close_SION[%03d]:\n",rank);
  }
  sion_fd_t * sfd= (sion_fd_t*) fdp;
  SION_CHECK(sion_parclose_mpi(sfd->filedesc), "could not close SION file");
  return;
}

void SION_Delete(char *testFileName, aiori_mod_opt_t * module_options)
{
  char errmsg[256];
  if (verbose >= VERBOSE_2) {
    printf("IOR_Delete_SION[%03d]: %s  (numfiles=%d)\n",rank,testFileName,sion_numfiles);
  }

  sprintf(errmsg,"[RANK %03d]:cannot delete file %s\n",rank,testFileName);
  if (unlink(testFileName) != 0) WARN(errmsg);
  if(sion_numfiles>1) {
    char fname[256];
    int filenum;
    for(filenum=1;filenum<sion_numfiles;filenum++) {
      sprintf(fname,"%s.%06d",testFileName,filenum);
      sprintf(errmsg,"[RANK %03d]:cannot delete file %s\n",rank,fname);
      if (unlink(fname) != 0) WARN(errmsg);
    }
  }
  return;
}

static char* SION_GetVersion()
{

  int main_version, sub_version, patch_level, fileformat_version;
  static char ver[1024]={};
  SION_CHECK(sion_get_version(&main_version, &sub_version, &patch_level, &fileformat_version),
    "cannot get SION version");
  sprintf(ver, "(%d.%d.%d.%d)",main_version,sub_version,patch_level, fileformat_version);
  return ver;
}

IOR_offset_t SION_GetFileSize(aiori_mod_opt_t * module_options, char *testFileName)
{

  IOR_offset_t aggFileSizeLocal, aggFileSizeGlobal;
  sion_fd_t * sfd = malloc(sizeof(sion_fd_t));
  memset(sfd, 0, sizeof(sion_fd_t));
  int sid;
  aggFileSizeGlobal=0;

  sid = sion_paropen_mpi(testFileName, SION_OPEN_READ,testComm, NULL);
  sion_seek(sid,0,SION_SEEK_END);
  aggFileSizeLocal = sion_tell(sid);
  MPI_CHECK(MPI_Allreduce(&aggFileSizeLocal,&aggFileSizeGlobal, 1, MPI_INT, MPI_SUM, testComm),"cannot open file to get file size");
  sion_parclose_mpi(sid);
  return(aggFileSizeGlobal);
}
