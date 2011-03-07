// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "CephFSInterface.h"

#include "client/libceph.h"
#include "common/config.h"
#include "msg/SimpleMessenger.h"
#include "common/Timer.h"

#include <sys/stat.h>

using namespace std;
const static int IP_ADDR_LENGTH = 24;//a buffer size; may want to up for IPv6.
static int path_size;
/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_initializeClient
 * Signature: (Ljava/lang/String;I)Z
 *
 * Performs any necessary setup to allow general use of the filesystem.
 * Inputs:
 *  jstring args -- a command-line style input of Ceph config params
 *  jint block_size -- the size in bytes to use for blocks
 * Returns: true on success, false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1initializeClient
  (JNIEnv *env, jobject obj, jstring j_args, jint block_size)
{
  dout(3) << "CephFSInterface: Initializing Ceph client:" << dendl;
  const char *c_args = env->GetStringUTFChars(j_args, 0);
  if (c_args == NULL) return false; //out of memory!
  string args(c_args);
  path_size = 64; //reasonable starting point?

  //construct an arguments vector
  vector<string> args_vec;
  string arg;
  size_t i = 0;
  size_t j = 0;
  bool local_writes = false;
  while (1) {
    j = args.find(' ', i);
    if (j == string::npos) {
      if (i == 0) { //there were no spaces? That can't happen!
	env->ReleaseStringUTFChars(j_args, c_args);
	return false;
      }
      //otherwise it's the last argument, so push it on and exit loop
      args_vec.push_back(args.substr(i, args.size()));
      break;
    }
    if (j!=i) { //if there are two spaces in a row, don't make a new arg
      arg = args.substr(i, j-i);
      if (arg.compare("set_local_pg") == 0)
	local_writes = true;
      else
	args_vec.push_back(arg);
    }
    i = j+1;
  }

  //convert to array
  const char ** argv = new const char*[args_vec.size()];
  for (size_t i = 0; i < args_vec.size(); ++i)
    argv[i] = args_vec[i].c_str();

  int r = ceph_initialize(args_vec.size(), argv);
  env->ReleaseStringUTFChars(j_args, c_args);
  delete argv;

  ceph_localize_reads(true);
  ceph_set_default_file_stripe_unit(block_size);
  ceph_set_default_object_size(block_size);

  if (r < 0) return false;
  r = ceph_mount();
  if (r < 0) return false;
  if (local_writes)
    ceph_set_default_preferred_pg(ceph_get_local_osd());
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getcwd
 * Signature: (J)Ljava/lang/String;
 *
 * Returns the current working directory.(absolute) as a jstring
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getcwd
  (JNIEnv *env, jobject obj)
{
  dout(10) << "CephFSInterface: In getcwd" << dendl;

  char *path = new char[path_size];
  int r = ceph_getcwd(path, path_size);
  if (r==-ERANGE) { //path is too short
    path_size = ceph_getcwd(path, 0) * 1.2; //leave some extra
    delete [] path;
    path = new char[path_size];
    ceph_getcwd(path, path_size);
  }
  jstring j_path = env->NewStringUTF(path);
  delete [] path;
  return j_path;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setcwd
 * Signature: (Ljava/lang/String;)Z
 *
 * Changes the working directory.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to switch to
 * Returns: true on success, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setcwd
(JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "CephFSInterface: In setcwd" << dendl;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL ) return false;
  jboolean success = (0 <= ceph_chdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_rmdir
 * Signature: (Ljava/lang/String;)Z
 *
 * Given a path to a directory, removes the directory.if empty.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the directory
 * Returns: true on successful delete; false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1rmdir
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "CephFSInterface: In rmdir" << dendl;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL ) return false;
  jboolean success = (0 == ceph_rmdir(c_path)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_path, c_path);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_unlink
 * Signature: (Ljava/lang/String;)Z
 * Given a path, unlinks it.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the file or empty dir
 * Returns: true if the unlink occurred, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1unlink
  (JNIEnv *env, jobject, jstring j_path)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  dout(10) << "CephFSInterface: In unlink for path " << c_path <<  ":" << dendl;
  int result = ceph_unlink(c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return (0 == result) ? JNI_TRUE : JNI_FALSE; 
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_rename
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 * Changes a given path name to a new name.
 * Inputs:
 *  jstring j_from: The path whose name you want to change.
 *  jstring j_to: The new name for the path.
 * Returns: true if the rename occurred, false otherwise
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1rename
  (JNIEnv *env, jobject, jstring j_from, jstring j_to)
{
  dout(10) << "CephFSInterface: In rename" << dendl;
  const char *c_from = env->GetStringUTFChars(j_from, 0);
  if (c_from == NULL) return false;
  const char *c_to   = env->GetStringUTFChars(j_to,   0);
  if (c_to == NULL) {
    env->ReleaseStringUTFChars(j_from, c_from);
    return false;
  }
  jboolean success = false;
  struct stat stbuf;
  if (ceph_lstat(c_to, &stbuf) < 0) //Hadoop doesn't want to overwrite files in a rename
    success = (0 <= ceph_rename(c_from, c_to)) ? JNI_TRUE : JNI_FALSE; 
  env->ReleaseStringUTFChars(j_from, c_from);
  env->ReleaseStringUTFChars(j_to, c_to);
  return success;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_exists
 * Signature: (Ljava/lang/String;)Z
 * Returns true if it the input path exists, false
 * if it does not or there is an unexpected failure.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1exists
(JNIEnv *env, jobject, jstring j_path)
{

  dout(10) << "CephFSInterface: In exists" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  dout(10) << "Attempting lstat with file " << c_path << ":" << dendl;
  int result = ceph_lstat(c_path, &stbuf);
  dout(10) << "result is " << result << dendl;
  env->ReleaseStringUTFChars(j_path, c_path);
  if (result < 0) {
    dout(10) << "Returning false (file does not exist)" << dendl;
    return JNI_FALSE;
  }
  else {
    dout(10) << "Returning true (file exists)" << dendl;
    return JNI_TRUE;
  }
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getblocksize
 * Signature: (Ljava/lang/String;)J
 * Get the block size for a given path.
 * Input:
 *  j_string j_path: The path (relative or absolute) you want
 *  the block size for.
 * Returns: block size (as a long) if the path exists, otherwise a negative
 *  number corresponding to the standard C++ error codes (which are positive).
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getblocksize
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In getblocksize" << dendl;

  //struct stat stbuf;
  
  jlong result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  // we need to open the file to retrieve the stripe size
  dout(10) << "CephFSInterface: getblocksize: opening file" << dendl;
  int fh = ceph_open(c_path, O_RDONLY);  
  env->ReleaseStringUTFChars(j_path, c_path);
  if (fh < 0) return fh;

  result = ceph_get_file_stripe_unit(fh);

  int close_result = ceph_close(fh);
  assert (close_result > -1);

  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_isfile
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the given path is a file; false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1isfile
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In isfile" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a file...
  if (0 > result) return false; 

  // check the stat result
  return (!(0 == S_ISREG(stbuf.st_mode)));
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_isdirectory
 * Signature: (Ljava/lang/String;)Z
 * Returns true if the given path is a directory, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1isdirectory
  (JNIEnv *env, jobject, jstring j_path)
{
  dout(10) << "In isdirectory" << dendl;

  struct stat stbuf;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_lstat(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);

  // if the stat call failed, it's definitely not a directory...
  if (0 > result) return JNI_FALSE; 

  // check the stat result
  return (!(0 == S_ISDIR(stbuf.st_mode)));
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getdir
 * Signature: (Ljava/lang/String;)[Ljava/lang/String;
 * Get the contents of a given directory.
 * Inputs:
 *  jstring j_path: The path (relative or absolute) to the directory.
 * Returns: A Java String[] of the contents of the directory, or
 *  NULL if there is an error (ie, path is not a dir). This listing
 *  will not contain . or .. entries.
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getdir
(JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In getdir" << dendl;

  // get the directory listing
  list<string> contents;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return NULL;
  DIR *dirp;
  int r;
  r = ceph_opendir(c_path, &dirp);
  if (r<0) {
    env->ReleaseStringUTFChars(j_path, c_path);
    return NULL;
  }
  int buflen = 100; //good default?
  char *buf = new char[buflen];
  string *ent;
  int bufpos;
  while (1) {
    r = ceph_getdnames(dirp, buf, buflen);
    if (r==-ERANGE) { //expand the buffer
      delete [] buf;
      buflen *= 2;
      buf = new char[buflen];
      continue;
    }
    if (r<=0) break;

    //if we make it here, we got at least one name
    bufpos = 0;
    while (bufpos<r) {//make new strings and add them to listing
      ent = new string(buf+bufpos);
      if (ent->compare(".") && ent->compare(".."))
	//we DON'T want to include dot listings; Hadoop gets confused
	contents.push_back(*ent);
      bufpos+=ent->size()+1;
      delete ent;
    }
  }
  delete [] buf;
  ceph_closedir(dirp);
  env->ReleaseStringUTFChars(j_path, c_path);
  
  if (r < 0) return NULL;

  // Create a Java String array of the size of the directory listing
  jclass stringClass = env->FindClass("java/lang/String");
  if (stringClass == NULL) {
    dout(0) << "ERROR: java String class not found; dying a horrible, painful death" << dendl;
    assert(0);
  }
  jobjectArray dirListingStringArray = (jobjectArray) env->NewObjectArray(contents.size(), stringClass, NULL);
  if(dirListingStringArray == NULL) return NULL;

  // populate the array with the elements of the directory list
  int i = 0;
  for (list<string>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    env->SetObjectArrayElement(dirListingStringArray, i, 
			       env->NewStringUTF(it->c_str()));
    ++i;
  }
  
  return dirListingStringArray;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_mkdirs
 * Signature: (Ljava/lang/String;I)I
 * Create the specified directory and any required intermediate ones with the
 * given mode.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1mkdirs
(JNIEnv *env, jobject, jstring j_path, jint mode)
{
  dout(10) << "In Hadoop mk_dirs" << dendl;

  //get c-style string and make the call, clean up the string...
  jint result;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_mkdirs(c_path, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  //...and return
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_append
 * Signature: (Ljava/lang/String;)I
 * Open a file to append. If the file does not exist, it will be created.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1append
(JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In hadoop open_for_append" << dendl;

  jint result;

  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_WRONLY|O_CREAT|O_APPEND);
  env->ReleaseStringUTFChars(j_path, c_path);

  return result;
}


/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_read
 * Signature: (Ljava/lang/String;)I
 * Open a file for reading.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1read
  (JNIEnv *env, jobject obj, jstring j_path)
{
  dout(10) << "In open_for_read" << dendl;

  jint result; 

  // open as read-only: flag = O_RDONLY
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_RDONLY);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_open_for_overwrite
 * Signature: (Ljava/lang/String;)I
 * Opens a file for overwriting; creates it if necessary.
 * Opening a dir is possible but may have bad results.
 * Inputs:
 *  jstring j_path: The path to open.
 *  jint mode: The mode to open with.
 * Returns: a jint filehandle, or a number<0 if an error occurs.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1open_1for_1overwrite
  (JNIEnv *env, jobject obj, jstring j_path, jint mode)
{
  dout(10) << "In open_for_overwrite" << dendl;

  jint result; 


  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  result = ceph_open(c_path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  // returns file handle, or -1 on failure
  return result;       
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_close
 * Signature: (I)I
 * Closes a given filehandle.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1close
(JNIEnv *env, jobject ojb, jint fh)
{
  dout(10) << "In CephTalker::ceph_close" << dendl;

  return ceph_close(fh);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setPermission
 * Signature: (Ljava/lang/String;I)Z
 * Change the mode on a path.
 * Inputs:
 *  jstring j_path: The path to change mode on.
 *  jint j_new_mode: The mode to apply.
 * Returns: true if the mode is properly applied, false if there
 *  is any error.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setPermission
(JNIEnv *env, jobject obj, jstring j_path, jint j_new_mode)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;
  int result = ceph_chmod(c_path, j_new_mode);
  env->ReleaseStringUTFChars(j_path, c_path);

  return (result==0);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_kill_client
 * Signature: (J)Z
 * 
 * Closes the Ceph client. This should be called before shutting down
 * (multiple times is okay but redundant).
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1kill_1client
  (JNIEnv *env, jobject obj)
{  
  ceph_deinitialize();  
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_stat
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/Stat;)Z
 * Get the statistics on a path returned in a custom format defined
 *  in CephTalker.
 * Inputs:
 *  jstring j_path: The path to stat.
 *  jobject j_stat: The stat object to fill.
 * Returns: true if the stat is successful, false otherwise.
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1stat
(JNIEnv *env, jobject obj, jstring j_path, jobject j_stat)
{
  //setup variables
  struct stat_precise st;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return false;

  jclass cls = env->GetObjectClass(j_stat);
  if (cls == NULL) return false;
  jfieldID c_size_id = env->GetFieldID(cls, "size", "J");
  if (c_size_id == NULL) return false;
  jfieldID c_dir_id = env->GetFieldID(cls, "is_dir", "Z");
  if (c_dir_id == NULL) return false;
  jfieldID c_block_id = env->GetFieldID(cls, "block_size", "J");
  if (c_block_id == NULL) return false;
  jfieldID c_mod_id = env->GetFieldID(cls, "mod_time", "J");
  if (c_mod_id == NULL) return false;
  jfieldID c_access_id = env->GetFieldID(cls, "access_time", "J");
  if (c_access_id == NULL) return false;
  jfieldID c_mode_id = env->GetFieldID(cls, "mode", "I");
  if (c_mode_id == NULL) return false;
  //do actual lstat
  int r = ceph_lstat_precise(c_path, &st);
  env->ReleaseStringUTFChars(j_path, c_path);

  if (r < 0) return false; //fail out; file DNE or Ceph broke

  //put variables from struct stat into Java
  env->SetLongField(j_stat, c_size_id, (long)st.st_size);
  env->SetBooleanField(j_stat, c_dir_id, (0 != S_ISDIR(st.st_mode)));
  env->SetLongField(j_stat, c_block_id, (long)st.st_blksize);
  env->SetLongField(j_stat, c_mod_id, (long long)st.st_mtime_sec*1000
		    +st.st_mtime_micro/1000);
  env->SetLongField(j_stat, c_access_id, (long long)st.st_atime_sec*1000
		    +st.st_atime_micro/1000);
  env->SetIntField(j_stat, c_mode_id, (int)st.st_mode);

  //return happy
  return true;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_statfs
 * Signature: (Ljava/lang/String;Lorg/apache/hadoop/fs/ceph/CephFileSystem/CephStat;)I
 * Statfs a filesystem in a custom format defined in CephTalker.
 * Inputs:
 *  jstring j_path: A path on the filesystem that you wish to stat.
 *  jobject j_ceph_stat: The CephStat object to fill.
 * Returns: true if successful and the CephStat is filled; false otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1statfs
(JNIEnv *env, jobject obj, jstring j_path, jobject j_cephstat)
{
  //setup variables
  struct statvfs stbuf;
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  jclass cls = env->GetObjectClass(j_cephstat);
  if (cls == NULL) return 1; //JVM error of some kind
  jfieldID c_capacity_id = env->GetFieldID(cls, "capacity", "J");
  jfieldID c_used_id = env->GetFieldID(cls, "used", "J");
  jfieldID c_remaining_id = env->GetFieldID(cls, "remaining", "J");

  //do the statfs
  int r = ceph_statfs(c_path, &stbuf);
  env->ReleaseStringUTFChars(j_path, c_path);


  if (r!=0) return r; //something broke

  //place info into Java; convert from bytes to kilobytes
  env->SetLongField(j_cephstat, c_capacity_id,
		    (long)stbuf.f_blocks*stbuf.f_bsize/1024);
  env->SetLongField(j_cephstat, c_used_id,
		    (long)(stbuf.f_blocks-stbuf.f_bavail)*stbuf.f_bsize/1024);
  env->SetLongField(j_cephstat, c_remaining_id,
		    (long)stbuf.f_bavail*stbuf.f_bsize/1024);
  return r;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_replication
 * Signature: (Ljava/lang/String;)I
 * Check how many times a path should be replicated (if it is
 * degraded it may not actually be replicated this often).
 * Inputs:
 *  jstring j_path: The path to check.
 * Returns: an int containing the number of times replicated.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1replication
(JNIEnv *env, jobject obj, jstring j_path)
{
  //get c-string of path, send off to libceph, release c-string, return
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if (c_path == NULL) return -ENOMEM;
  int replication = ceph_get_file_replication(c_path);
  env->ReleaseStringUTFChars(j_path, c_path);
  return replication;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_hosts
 * Signature: (IJ)Ljava/lang/String;
 * Find the IP:port addresses of the primary OSD for a given file and offset.
 * Inputs:
 *  jint j_fh: The filehandle for the file.
 *  jlong j_offset: The offset to get the location of.
 * Returns: a jstring of the location as IP, or NULL if there is an error.
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1hosts
(JNIEnv *env, jobject obj, jint j_fh, jlong j_offset)
{
  //get the address
  char *address = new char[IP_ADDR_LENGTH];
  int r = ceph_get_file_stripe_address(j_fh, j_offset, address, IP_ADDR_LENGTH);
  if (r == -ERANGE) {//buffer's too small
    delete [] address;
    int size = ceph_get_file_stripe_address(j_fh, j_offset, address, 0);
    address = new char[size];
    r = ceph_get_file_stripe_address(j_fh, j_offset, address, size);
  }
  if (r != 0) { //some rather worse problem
    if (r == -EINVAL) return NULL; //ceph thinks there are no OSDs
  }
  //make java String of address
  jstring j_addr = env->NewStringUTF(address);
  delete [] address;
  return j_addr;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_setTimes
 * Signature: (Ljava/lang/String;JJ)I
 * Set the mtime and atime for a given path.
 * Inputs:
 *  jstring j_path: The path to set the times for.
 *  jlong mtime: The mtime to set, in millis since epoch (-1 to not set).
 *  jlong atime: The atime to set, in millis since epoch (-1 to not set)
 * Returns: 0 if successful, an error code otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1setTimes
(JNIEnv *env, jobject obj, jstring j_path, jlong mtime, jlong atime)
{
  const char *c_path = env->GetStringUTFChars(j_path, 0);
  if(c_path == NULL) return -ENOMEM;

  //build the mask for ceph_setattr
  int mask = 0;
  if (mtime!=-1) mask = CEPH_SETATTR_MTIME;
  if (atime!=-1) mask |= CEPH_SETATTR_ATIME;
  //build a struct stat and fill it in!
  //remember to convert from millis to seconds and microseconds
  stat_precise attr;
  attr.st_mtime_sec = mtime / 1000;
  attr.st_mtime_micro = (mtime % 1000) * 1000;
  attr.st_atime_sec = atime / 1000;
  attr.st_atime_micro = (atime % 1000) * 1000;
  return ceph_setattr_precise(c_path, &attr, mask);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_read
 * Signature: (JI[BII)I
 * Reads into the given byte array from the current position.
 * Inputs:
 *  jint fh: the filehandle to read from
 *  jbyteArray j_buffer: the byte array to read into
 *  jint buffer_offset: where in the buffer to start writing
 *  jint length: how much to read.
 * There'd better be enough space in the buffer to write all
 * the data from the given offset!
 * Returns: the number of bytes read on success (as jint),
 *  or an error code otherwise.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1read
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In read" << dendl;


  // Make sure to convert the Hadoop read arguments into a
  // more ceph-friendly form
  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the read
  result = ceph_read((int)fh, c_buffer, length, -1);

  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);
  
  return result;
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_seek_from_start
 * Signature: (JIJ)J
 * Seeks to the given position in the given file.
 * Inputs:
 *  jint fh: The filehandle to seek in.
 *  jlong pos: The position to seek to.
 * Returns: the new position (as a jlong) of the filehandle on success,
 *  or a negative error code on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1seek_1from_1start
  (JNIEnv *env, jobject obj, jint fh, jlong pos)
{
  dout(10) << "In CephTalker::seek_from_start" << dendl;

  return ceph_lseek(fh, pos, SEEK_SET);
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_getpos
 * Signature: (I)J
 *
 * Get the current position in a file (as a jlong) of a given filehandle.
 * Returns: jlong current file position on success, or a
 *  negative error code on failure.
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getpos
  (JNIEnv *env, jobject obj, jint fh)
{
  dout(10) << "In CephTalker::ceph_getpos" << dendl;

  // seek a distance of 0 to get current offset
  return ceph_lseek(fh, 0, SEEK_CUR);  
}

/*
 * Class:     org_apache_hadoop_fs_ceph_CephTalker
 * Method:    ceph_write
 * Signature: (I[BII)I
 * Write the given buffer contents to the given filehandle.
 * Inputs:
 *  jint fh: The filehandle to write to.
 *  jbyteArray j_buffer: The buffer to write from
 *  jint buffer_offset: The position in the buffer to write from
 *  jint length: The number of (sequential) bytes to write.
 * Returns: jint, on success the number of bytes written, on failure
 *  a negative error code.
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1write
  (JNIEnv *env, jobject obj, jint fh, jbyteArray j_buffer, jint buffer_offset, jint length)
{
  dout(10) << "In write" << dendl;

  // IMPORTANT NOTE: Hadoop write arguments are a bit different from POSIX so we
  // have to convert.  The write is *always* from the current position in the file,
  // and buffer_offset is the location in the *buffer* where we start writing.
  jint result; 

  // Step 1: get a pointer to the buffer.
  jbyte *j_buffer_ptr = env->GetByteArrayElements(j_buffer, NULL);
  if (j_buffer_ptr == NULL) return -ENOMEM;
  char *c_buffer = (char*) j_buffer_ptr;

  // Step 2: pointer arithmetic to start in the right buffer position
  c_buffer += (int)buffer_offset;

  // Step 3: do the write
  result = ceph_write((int)fh, c_buffer, length, -1);
  
  // Step 4: release the pointer to the buffer
  env->ReleaseByteArrayElements(j_buffer, j_buffer_ptr, 0);

  return result;
}
