<?php

/**
 * TODO: 
 *
 * public vs. private ACLs
 * optional buffering on disk rather than in RAM
 * Grab the first 60K or so, then the rest in a separate request, so getimagesize() isn't slow
 * Use of ../ within a URL should be allowed for ease of coding
 *
 * A stream wrapper for Amazon S3 based on Amazon's offical PHP API. 
 * Amazon S3 files can be accessed at s3://bucketname/path/to/object. Unlike other 
 * wrappers, this wrapper supports opendir/readdir/closedir for any subdirectory level
 * (to the limit of S3 key length). Although S3 does not have a concept exactly matching 
 * subdirectories, it can do prefix matches and return common prefixes, which is just as good. 
 * Just keep in mind that you don't really have to create or remove directories. mkdir and rmdir 
 * return success and do nothing (however, rmdir fails if the "directory" is not empty in order to
 * better emulate what regular filesystems do since some code may rely on this behavior).
 * Any subdirectories returned by readdir() have a trailing / attached to allow both your code
 * and the stat() function to distinguish them from files without an expensive network call.
 *
 * THERE IS A 5GB LIMIT ON EACH FILE, AND YOUR PHP MEMORY LIMIT WILL PROBABLY STOP YOU LONG
 * BEFORE YOU GET THERE, since currently everything is read and written as a complete file and
 * buffered in its entirety in memory. I'm looking into changing this, but one step at a time.
 *
 * Usage (with a public ACL so people can see your files via the web):
 *
 * $wrapper = new aS3StreamWrapper();
 * $wrapper->register(array('key' => 'xyz', 'secretKey' => 'abc', 'region' => AmazonS3::REGION_US_E1, 'acl' => AmazonS3::ACL_PUBLIC));
 * Now fopen("s3://mybucket/path/to/myobject.txt", "r") and friends work.
 *
 * Built for Apostrophe apostrophenow.com
 *
 * @class aS3StreamWrapper
 * @license BSD
 * @author tom@punkave.com Tom Boutell of P'unk Avenue
 */
 
require dirname(__FILE__) . '/../vendor/aws-sdk/sdk.class.php';
require dirname(__FILE__) . '/aS3StreamWrapperMimeTypes.class.php';

class aS3StreamWrapper
{
  /**
   * Create an object of this class with new and then call register() on it.
   *
   * Internal dev note: supposedly the constructor of a stream wrapper does not get called
   * except on stream_open, so keep that in mind
   */
  public function __construct()
  {
  }
  
  /**
   * Options specified in the register() call. These can be overridden 
   * for individual streams using a stream context
   */
  static protected $protocolOptions;

  /**
   * Stream context, set by fopen and friends if stream_create_context was used.
   * We can call stream_context_get_options on this
   */
  public $context;

  /**
   * Final set of options arrived at by merging the above
   */
  protected $options;
  
  /**
   * Protocol name. This is usually s3 but you can register more than one, and
   * we use this to distinguish sets of options. Usually $this->init() sets this
   * from the $path but the rename() operation, which takes two paths, sets it
   * on its own
   */
  protected $protocol;
  
  /**
   * Returns options set at register() time, unless overridden by
   * options set with stream_context_create() for this particular stream.
   * Looks for options in the 's3' key of the array passed to stream_context_create()
   */
  
  protected function getOption($o, $default = null)
  {
    if (!isset($this->options))
    {
      $this->options = array();
      if (isset(self::$protocolOptions[$this->protocol]))
      {
        $this->options = self::$protocolOptions[$this->protocol];
      }
      if ($this->context)
      {
        $streamOptions = stream_context_get_options($this->context);
        if (isset($streamOptions['s3']))
        {
          $this->options = array_merge($this->options, $streamOptions['s3']);
        }
      }
    }
    if (isset($this->options[$o]))
    {
      return $this->options[$o];
    }
    return $default;
  }

  protected function getRegion()
  {
    return $this->getOption('region', AmazonS3::REGION_US_E1);
  }
  
  /**
   * Register the stream wrapper. Options passed here are available to
   * the stream wrapper methods at any time via getOption. Note that you can
   * register two different s3 "protocols" with different credentials (the default
   * protocol name is s3). 
   *
   * The following options are required, either here or in stream_create_context:
   * 'key', 'secretKey'
   *
   * The optional 'region' option specifies what Amazon S3 region to create
   * new buckets in, if you choose to create buckets with mkdir() calls.
   * It defaults to AmazonS3::REGION_US_E1. See services/s3.class.php in the SDK
   * 
   * The optional 'protocol' option changes the name of the protocol from s3 to
   * something else. You can register multiple protocols
   * 
   */
  public function register(array $options = array())
  {
    // Every protocol gets its own set of options
    $protocol = isset($options['protocol']) ? $options['protocol'] : 's3';
    self::$protocolOptions[$protocol] = $options;
    
    stream_wrapper_register($protocol, get_class($this));
  }

  /**
   * Directory listing data doled out by readdir
   */
  protected $dirInfo = false;
  
  /**
   * Offset into directory data
   */
  protected $dirOffset = 0;
  
  /**
   * Array with protocol, bucket and path keys once init() is called successfully
   */
  protected $info = false;
  
  /**
   * Amazon S3 service objects. Usually just one exists but if you make
   * requests with custom credentials via a stream context or multiple
   * protocol registrations more than one can be created
   */
  static protected $services = array();

  /**
   * Bust up the path of interest into its component parts.
   * The "site" name must be a bucket name. The path (key) will
   * always be at least / for consistency
   */
  protected function init($path)
  {
    $info = $this->parse($path);
    if (!$info)
    {
      return false;
    }
    $this->info = $info;
    $this->protocol = $info['protocol'];
    return true;
  }
  
  protected function parse($path)
  {
    $info = array();
    $parsed = parse_url($path);
    if (!$parsed)
    {
      return false;
    }
    $info['protocol'] = $parsed['scheme'];
    $info['bucket'] = $parsed['host'];
    // No leading / in S3 (otherwise our public S3 URLs are strange)
    if (isset($parsed['path']))
    {
      $info['path'] = substr($parsed['path'], 1);
      // Lame: substr() returns false, not the empty string, if you
      // attempt to take an empty substring starting right after the end
      if ($info['path'] === false)
      {
        $info['path'] = '';
      }
    }
    else
    {
      $info['path'] = '';
    }
    return $info;
  }
  
  /**
   * Allow separate S3 objects for separate credentials but don't
   * make redundant S3 objects
   */
  protected function getService()
  {
    $id = $this->getOption('key', '') . ':' . $this->getOption('secretKey', '') . ':' . $this->getOption('token', '');
    if (!isset(self::$services[$id]))
    {
      self::$services[$id] = new AmazonS3($this->getOption('key'), $this->getOption('secretKey'), $this->getOption('token'));
    }
    return self::$services[$id];
  }
  
  protected $dirResults = null;
  protected $dirPosition = 0;
  
  /**
   * Implements opendir(). Pulls a list of "files" and "directories" at
   * $path from S3 and preps them to be returned one by one by readdir().
   * Note that directories are suffixed with a / to distinguish them
   */
  public function dir_opendir ($path, $optionsDummy)
  {
    if (!$this->init($path))
    {
      return false;
    }
    $this->dirResults = $this->getDirectoryListing($this->info);
    if ($this->dirResults === false)
    {
      $this->dirResults = null;
      return false;
    }
    $this->dirPosition = 0;
    return true;
  }
  
  /**
   * Set up options array for a call to list_objects. If delimited
   * is true, return "subdirectories" plus "files" at this level,
   * rather than all objects
   */
  protected function getOptionsForDirectory($options = array())
  {
    $s3Options = array();
    // Usually the path is the single path this operation cares about, but not always
    $path = isset($options['path']) ? $options['path'] : $this->info['path'];
    // Append a / unless we are listing items at the root
    if (strlen($path) && (!preg_match('/\/$/', $path)))
    {
      $path .= '/';
    }
    $s3Options['prefix'] = $path;
    if (isset($options['delimited']) && (!$options['delimited']))
    {
      // No delimiter wanted (for instance, we want a simple "are there any files darn it" test on just one XML query)
    }
    else
    {
      // Normal case: return everything in the same "subdirectory" as a subdirectory
      $s3Options['delimiter'] = '/';
    }
    return $s3Options;
  }
  
  protected function getDirectoryListing($info = null, $options = array())
  {
    if ($info === null)
    {
      $info = $this->info;
    }
    $options = $this->getOptionsForDirectory(array_merge($options, array('path' => $info['path'])));
		$results = array();

    // Markers can be fetched more than once according to the spec, don't return them twice,
    // but don't blindly assume we scan skip the first result either in case they surprise us
    $have = array();
    
		do
		{
			$list = $this->getService()->list_objects($info['bucket'], $options);
			if (!$list->isOK())
			{
			  return false;
			}
			// Subdirectories
			$keys = $list->body->query('descendant-or-self::Prefix');
			if ($keys)
			{
  			foreach ($keys as $key)
  			{
  			  $key = (string) $key;
  			  if (strlen($key) <= strlen($options['prefix']))
  			  {
  			    // S3 tells us about the directory itself as a prefix, which is not interesting
  			    continue;
  			  }
  			  // results of readdir() do not include the path, just the basename
  			  $key = substr($key, strlen($options['prefix']));
  			  if (!isset($have[$key]))
  			  {
    			  // Make sure there is no XML object funny business returned
    			  // Leave the delimiter attached, it allows us to identify
    			  // directories without more network calls
    			  $results[] = $key;
    			  $have[$key] = true;
    			}
  			}
  		}
			// Files
			$keys = $list->body->query('descendant-or-self::Key');
			if ($keys)
			{
  			foreach ($keys as $key)
  			{
  			  $key = (string) $key;
  			  // results of readdir() do not include the path, just the basename
  			  if (strlen($key) <= strlen($options['prefix']))
  			  {
  			    // If something is both a file and a directory - possible in s3 where directories
  			    // are virtual - it could show up in its own listing. This tends to result in 
  			    // nasty infinite loops in recursive delete functions etc. Defend against this by
  			    // not returning it
  			    continue;
  			  }
  			  $key = substr($key, strlen($options['prefix']));
  			  
  			  if (!isset($have[$key]))
  			  {
    			  // Make sure there is no XML object funny business returned
    			  $results[] = (string) $key;
    			  $have[$key] = true;
    			}
  			}
  		}
  		// Pick up where we left off
			$options = array_merge($options, array('marker' => end($results)));
		} while (((string) $list->body->IsTruncated) === 'true');
    return $results;
  }

  /**
   * Implements readdir(), reading the name of the next file or subdirectory
   * in the directory or returning false if there are no more or opendir() was 
   * never called. Subdirectories returned are suffixed with '/' to distinguish them 
   * from files without repeated API calls
   */
  public function dir_readdir()
  {
    if (isset($this->dirResults))
    {
      if ($this->dirPosition < count($this->dirResults))
      {
        return $this->dirResults[$this->dirPosition++];
      }
    }
    return false;
  }
  
  /**
   * Implements closedir(), closing the directory listing
   */
  public function dir_closedir()
  {
    $this->dirResults = null;
    $this->dirPosition = 0;
    return true;
  }
  
  /**
   * Rewind to start of directory listing so we can start calling
   * readdir again from the top
   */
  public function dir_rewinddir()
  {
    if (isset($this->dirResults))
    {
      $this->dirPosition = 0;
      return true;
    }
    return false;
  }
  
  /**
   * Implements mkdir for the s3 protocol
   * Make a directory. If $path is s3://bucketname or s3://bucketname/
   * with no subdirectory name, we attempt to create that bucket and
   * return failure if it already exists or it otherwise cannot be made.
   * Buckets are created in the region specified by the 
   * region option when register() is called, defaulting to 
   * AmazonS3::REGION_US_E1 (see services/s3.class.php in the SDK).
   *
   * If there is a subdirectory name, we always return success since
   * you don't really have to create common prefixes with S3, they 
   * just work. Note that in this case we assume the bucket already exists for 
   * performance reasons (if it isn't you'll find out soon enough when
   * you try to manipulate files or read directory contents).
   */
  public function mkdir($path, $mode, $options)
  {
    if (!$this->init($path))
    {
      return false;
    }
    $path = $this->info['path'];
    if ($path === '')
    {
      return $this->getService()->create_bucket($this->info['bucket'], $this->getRegion())->isOK();
    }
    // Subdirectory creation always succeeds because subdirectories are implemented
    // using the prefix/delimiter mechanism, which doesn't require creating anything first
    return true;
  }

  /**
   * Implements rmdir for the s3 protocol
   * Remove a directory. If the URL is s3://bucketname/ or just s3://bucketname we
   * attempt to remove the entire bucket, returning failure if it is not empty or
   * otherwise not a valid bucket to delete. If the URL has a subdirectory in it,
   * we just return success as long as the subdirectory is not empty, because this is what 
   * other file systems do, and some code may use it as a test. S3 doesn't really
   * need us to physically delete a "directory" since it does not have directory
   * objects, just a prefix/delimiter mechanism for queries. But let's emulate 
   * the semantics as closely as possible
   */
  public function rmdir($path, $options)
  {
    if (!$this->init($path))
    {
      return false;
    }
    
    $path = $this->info['path'];
    if ($path === '')
    {
      // On success this returns a CFResponse, on failure it returns false.
      // Convert the CFResponse to plain old true
      return !!$this->getService()->delete_bucket($this->info['bucket']);
    }
    if ($this->hasDirectoryContents())
    {
      return false;
    }
		
    return true;
  }

  protected function hasDirectoryContents()
  {
    $list = $this->getService()->list_objects($this->info['bucket'], array_merge($this->getOptionsForDirectory(array('delimited' => false)), array('max-keys' => 1)));
		
		$keys = $list->body->query('descendant-or-self::Key');
		return !!count($keys);
  }
  
  /**
   * Implement unlink() for the s3 protocol. Removes files only, not folders or buckets
   * (see rmdir()).
   */
  
  public function unlink($path)
  {
    if (!$this->init($path))
    {
      return false;
    }
    return $this->getService()->delete_object($this->info['bucket'], $this->info['path'])->isOK();
  }
   
  /**
   * Implement rename() for the s3 protocol
   * Rename a file or directory. WARNING: S3 does NOT have a native rename feature,
   * so this method must COPY EVERYTHING INVOLVED. If you rename a bucket, the
   * ENTIRE BUCKET MUST BE COPIED. If you copy a subdirectory, everything in that
   * subdirectory must be copied, etc. That equals a lot of S3 traffic. 
   *
   * For safety, this method does not delete the old material at $from until the copy operation has
   * completely succeeded. 
   *
   * If, after the copy has completely succeeded, there are errors during the deletion
   * of the source or its contents, this method returns false but the new copy remains
   * in place along with whatever portions of the old copy could not be removed. Otherwise
   * you could be left with no way to recover a portion of your data.
   *
   * THERE IS A 5GB LIMIT ON THE SIZE OF INDIVIDUAL OBJECTS INVOLVED IN A rename() OPERATION.
   * This is a limitation of the copy_object API in Amazon S3.
   */
   
  public function rename($from, $to)
  {
    $fromInfo = $this->parse($from);
    if (!$fromInfo)
    {
      return false;
    }
    $this->protocol = $fromInfo['protocol'];
    $toInfo = $this->parse($to);
    if (!$toInfo)
    {
      return false;
    }
    if ($fromInfo['protocol'] !== $toInfo['protocol'])
    {
      // You cannot "rename" across protocols
      return false;
    }

    $service = $this->getService();

    // See if this is a simple copy of an object. If $from is an object rather than a bucket or
    // subdirectory then this operation will succeed. Don't try this if either from or to is
    // the root of a bucket
    
    if (strlen($fromInfo['path']) && strlen($toInfo['path']))
    {
      if ($service->copy_object(array('bucket' => $fromInfo['bucket'], 'filename' => $fromInfo['path']), 
        array('bucket' => $toInfo['bucket'], 'filename' => $toInfo['path']))->isOK())
      {
        // That worked so delete the original
        if ($service->delete_object($fromInfo['bucket'], $fromInfo['path'])->isOK())
        {
          return true;
        }
        // The delete failed, but the copy succeeded. No way to be that specific in our error message
        return false;
      }
    }

    $createdBucket = true;

    // If $to is the root of a bucket, create the bucket
    if ($toInfo['path'] === '')
    {
      if (!$service->create_bucket($toInfo['bucket'], $this->getRegion())->isOK())
      {
        return false;
      }
    }
    
    // Get a full list of objects at $from 
    
    $objects = $this->getDirectoryListing($fromInfo, array('delimited' => false));
    if ($objects === false)
    {
      if ($createdBucket)
      {
        $service->delete_bucket($toInfo['bucket']);
      }
      return false;
    }
    
    $fromPaths = array();
    $toPaths = array();
    foreach ($objects as $object)
    {
      if (strlen($fromInfo['path']))
      {
        $fromPaths[] = $fromInfo['path'] . '/' . $object;
      }
      else
      {
        $fromPaths[] = $object;
      }
      if (strlen($toInfo['path']))
      {
        $toPaths[] = $toInfo['path'] . '/' . $object;
      }
      else
      {
        $toPaths[] = $object;
      }
    }
    
    // and copy them all to $to
    
    for ($i = 0; ($i < count($objects)); $i++)
    {
      if (!$service->copy_object(array('bucket' => $fromInfo['bucket'], 'filename' => $fromPaths[$i]), array('bucket' => $toInfo['bucket'], 'filename' => $toPaths[$i]))->isOK())
      {
        for ($j = 0; ($j < $i); $j++)
        {
          $service->delete_object($toInfo['bucket'], $toPaths[$j]);
        }
        if ($createdBucket)
        {
          $service->delete_bucket($toInfo['bucket']);
        }
        return false;
      }
    }
    
    // BEGIN DELETION UNDER THE ORIGINAL NAME
    
    // Once we get started with the deletions of the old copy it is better not to delete the
    // new copy if something goes wrong, because then we have no copies at all.
    
    for ($i = 0; ($i < count($objects)); $i++)
    {
      if (!$service->delete_object($fromInfo['bucket'], $fromPaths[$i])->isOK())
      {
        return false;
      }
    }

    // If $from is the root of a bucket delete the old bucket
    if ($fromInfo['path'] === '')
    {
      if (!$service->delete_bucket($fromInfo['bucket']))
      {
        return false;
      }
    }  
    return true;
  }
  
  /**
   * s3 does not have a select() operation, so we can't cast to a resource
   */
  public function stream_cast ($cast_as)
  {
    return false;
  }
  
  /**
   * Data to be written to or read from a stream. Alas S3's limited semantics pretty much
   * require we read or write the entire object at a time (even if it's massive) which leads
   * to practical limitations due to memory usage. Possibly we can use multipart upload later
   * to ameliorate this in the case of writing big new objects
   * https://forums.aws.amazon.com/thread.jspa?threadID=10752&start=25&tstart=0
   */
  protected $data = null;
  
  /**
   * Offset into the data of the seek pointer at this time
   */
  protected $dataOffset = 0;
  
  /**
   * True if the data was modified in any way and therefore we must write on close
   */
  protected $dirty = false;
  
  /**
   * Whether we are expecting read operations
   */
  protected $read = false;
  
  /**
   * Whether we are expecting write operations
   */
  protected $write = false;
  
  /**
   * Opens a stream, as in fopen() or file_get_contents()
   */
  public function stream_open ($path, $mode, $options, &$opened_path)
  {
    if (!$this->init($path))
    {
      return false;
    }
    $end = false;
    $modes = array_flip(str_split($mode));
    if (isset($modes['r']))
    {
      $this->read = true;
      $this->write = false;
    }
    elseif (isset($modes['a']))
    {
      $this->read = true;
      $this->write = true;
      $end = true;
    }
    elseif (isset($modes['w']))
    {
      // Read nothing in, get ready to write to the buffer
      $this->read = false;
      $this->write = true;
    }
    elseif (isset($modes['x']))
    {
      $this->read = false;
      $this->write = true;
      $response = $this->getService()->get_object_headers($this->info['bucket'], $this->info['path']);
      if ($response->isOK())
      {
        // x does not allow opening an existing file
        return false;
      }
    }
    elseif (isset($modes['c']))
    {
      $this->read = false;
      $this->write = true;
    }
    else
    {
      // Unsupported mode
      return false;
    }
    if (isset($modes['+']))
    {
      $this->read = true;
      $this->write = true;
    }
    $this->data = '';
    $this->dataOffset = 0;
    $this->dirty = false;
    if ($this->read || isset($modes['c']))
    {
      $result = $this->getService()->get_object($this->info['bucket'], $this->info['path']);
      if (!$result->isOK())
      {
        if ($end)
        {
          // It's OK if an append operation starts a new file
          // Mark it dirty so we know the creation of the file is needed even if
          // nothing gets written to it
          $this->dirty = true;
          $openedPath = $path;
          return true;
        }
        else
        {
          // Otherwise failure to find an existing object here is an error
          return false;
        }
      }
      $this->data = (string) $result->body;
      if ($end)
      {
        $this->dataOffset = strlen($this->data);
      }
    }
    $openedPath = $path;
    return true;
  }
  
  /**
   * Close a stream opened with stream_open. Implements fclose() and is also closed by
   * file_put_contents and the like
   */
  public function stream_close()
  {
    if (is_null($this->data))
    {
      // No stream open
      return false;
    }
    $result = $this->stream_flush();
    // If this distresses you should call fflush separately first and make sure it works.
    // That's necessary with any filesystem in principle although we rarely bother
    // to check with the regular filesystem (and then we get busted by "disk full")
    $this->data = null;
    return $result;
  }
  
  /**
   * Flush any unstored data in the buffer to S3. Implements fflush() and is used by stream_close
   */
  public function stream_flush()
  {
    if ($this->write)
    {
      if ($this->dirty)
      {
        $acl = $this->getOption('acl');
        $response = $this->getService()->create_object($this->info['bucket'], $this->info['path'], array('body' => $this->data, 'acl' => $this->getOption('acl'), 'contentType' => $this->getMimeType($this->info['path'])));
        if (!$response->isOK())
        {
          return false;
        }
      }
    }
    return true;
  }
  
  /**
   * Returns true if we are at the end of a stream.
   *
   */
  public function stream_eof()
  {
    return (strlen($this->data) === $this->dataOffset);
  }
  
  /**
   * You can't lock an S3 "file"
   */
  public function stream_lock($operation)
  {
    return false;
  }
  
  /**
   * You can't unlock an S3 "file"
   */
  public function stream_unlock($operation)
  {
    return false;
  }
  
  /**
   * Someday: stream_metadata. Doesn't exist in 5.3
   */
   
  /**
   * Read specified # of bytes. Implements fread() among other things
   */
  public function stream_read($bytes)
  {
    if (!$this->read)
    {
      // Not supposed to be reading
      return false;
    }
    $total = strlen($this->data);
    $remaining = $total - $this->dataOffset;
    if ($bytes > $remaining)
    {
      $bytes = $remaining;
    }
    $result = substr($this->data, $this->dataOffset, $bytes);
    $this->dataOffset += $bytes;
    return $result;
  }

  /**
   * Write specified # of bytes. Implements fwrite() among other things
   */
  public function stream_write($data)
  {
    if (!$this->write)
    {
      // Not supposed to be writing
      return 0;
    }
    $len = strlen($data);
    $this->data = substr_replace($this->data, $data, $this->dataOffset);
    $this->dataOffset += $len;
    $this->dirty = true;
    return $len;
  }
  
  /**
   * Seek to the specified poin the buffer. Implements fseek(), sort of.
   * PHP will sometimes just adjust its own read buffer instead
   */
  public function seek($offset, $whence)
  {
    $len = strlen($this->data);
    $newOffset = 0;
    if ($whence === SEEK_SET)
    {
      $newOffset = $offset;
    }
    elseif ($whence === SEEK_CUR)
    {
      $newOffset += $offset;
    }
    elseif ($whence === SEEK_END)
    {
      $newOffset = $len + $offset;
    }
    else
    {
      // Unknown whence value
      return false;
    }
    if ($newOffset < 0)
    {
      return false;
    }
    if ($newOffset > $len)
    {
      return false;
    }
    $this->dataOffset = $newOffset;
    return true;
  }
  
  /**
   * Nonblocking and such. We don't support this at this time.
   * It's possible to have a read timeout. Maybe later after we
   * get around to pulling files in chunks rather than all at once
   */
  public function stream_set_option($option, $arg1, $arg2)
  {
    return false;
  }

  /**
   * Implements stat($url)
   */
  public function url_stat($path, $flags)
  {
    if (!$this->init($path))
    {
      return false;
    }
    return $this->stream_stat();
  }
  
  /**
   * Implements fstat($resource) - stat on an already opened file
   */
  public function stream_stat()
  {
    if (is_null($this->info))
    {
      // No file open
      return false;
    }
    $dir = false;
    if ($this->info['path'] === '')
    {
      // We want to know about the bucket
      if ($this->getService()->if_bucket_exists($this->info['bucket']))
      {
        $dir = true;
      }
    }
    if ((!$dir) && (preg_match('/\/$/', $this->info['path'])))
    {
      $dir = true;
    }
    else
    {
      $response = $this->getService()->get_object_headers($this->info['bucket'], $this->info['path']);
      if (!$response->isOK())
      {
        // Hmm. Let's take another shot at this possibly being a folder
        if ($this->hasDirectoryContents())
        {
          $dir = true;
        }
        else
        {
          return false;
        }
      }
    }
    if ($dir)
    {
      // Paths ending in a slash are always considered folders, and folders don't need to be
      // explicitly created in S3
      $mode = 0040000 + 0777;
      $mtime = time();
      $size = 0;
    }
    else
    {
      if (!isset($response->header['last-modified']))
      {
        echo("Path was " . $this->info['path'] . "\n");
        var_dump($response);
        exit(1);
      }
      $mtime = strtotime($response->header['last-modified']);
      $size = (int) $response->header['content-length'];
      // Bitflags for st_mode indicating a regular file that everyone can read/write/execute
      $mode = 0100000 + 0777;
    }
    return array(
      // 0	dev	device number
      0,
      // 1	ino	inode number
      0,
      // Permissions and file type
      $mode,
      // nlink	number of links 1 is a reasonable value
      1,
      // uid of owner
      0,
      // gid of owner
      0,
      // device type, if inode device
      0,
      // size in bytes
      $size,
      // atime time of last access (unix timestamp). Most systems, including Linux, don't really maintain this separately from mtime
      $mtime,
      // mtime time of last modification (unix timestamp)
      $mtime,
      // ctime time of last inode change (unix timestamp)
      $mtime,
      // blksize blocksize of filesystem IO (-1 where not relevant)
      -1,
      // blocks number of 512-byte blocks allocated (-1 where not relevant)
      -1,
      'dev' => 0,
      'ino' => 0,
      'mode' => $mode,
      'nlink' => 1,
      'uid' => 0,
      'gid' => 0,
      'rdev' => 0,
      'size' => $size,
      'atime' => $mtime,
      'mtime' => $mtime,
      'ctime' => $mtime,
      'blksize' => -1,
      'blocks' => -1
    );
  }
  
  /**
   * Override me if you hate our mime types list
   */
  public function getMimeType($path)
  {
    $dot = strrpos($path, '.');
    if ($dot !== false)
    {
      $extension = substr($path, $dot + 1);
    }
    else
    {
      $extension = '';
    }
    if (isset(aS3StreamWrapperMimeTypes::$mimeTypes[$extension]))
    {
      return aS3StreamWrapperMimeTypes::$mimeTypes[$extension];
    }
    else
    {
      return 'application/octet-stream';
    }
  }
}
