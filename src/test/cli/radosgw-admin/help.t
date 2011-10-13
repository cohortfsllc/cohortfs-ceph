  $ radosgw-admin --help
  usage: radosgw-admin <cmd> [options...]
  commands:
    user create                create a new user
    user modify                modify user
    user info                  get user info
    user rm                    remove user
    user suspend               suspend a user
    user enable                reenable user after suspension
    subuser create             create a new subuser
    subuser modify             modify subuser
    subuser rm                 remove subuser
    key create                 create access key
    key rm                     remove access key
    bucket list                list buckets
    bucket link                link bucket to specified user
    bucket unlink              unlink bucket from specified user
    bucket stats               returns bucket statistics
    pool add                   add an existing pool to those which can store buckets
    pool info                  show pool information
    pool create                generate pool information (requires bucket)
    policy                     read bucket/object policy
    log list                   list log objects
    log show                   dump a log from specific object or (bucket + date
                               + bucket-id)
    log rm                     remove log object
    temp remove                remove temporary objects that were created up to
                               specified date (and optional time)
  options:
     --uid=<id>                user id
     --auth-uid=<auid>         librados uid
     --subuser=<name>          subuser name
     --access-key=<key>        S3 access key
     --email=<email>
     --secret=<key>            specify secret key
     --gen-access-key          generate random access key (for S3)
     --gen-secret              generate random secret key
     --key-type=<type>         key type, options are: swift, s3
     --access=<access>         Set access permissions for sub-user, should be one
                               of read, write, readwrite, full
     --display-name=<name>
     --bucket=<bucket>
     --pool=<pool>
     --object=<object>
     --date=<yyyy-mm-dd>
     --time=<HH:MM:SS>
     --bucket-id=<bucket-id>
     --format=<format>         specify output format for certain operations: xml,
                               json
     --purge-data              when specified, user removal will also purge all the
                               user data
  --conf/-c        Read configuration from the given configuration file
  -d               Run in foreground, log to stderr.
  -f               Run in foreground, log to usual location.
  --id/-i          set ID portion of my name
  --name/-n        set name (TYPE.ID)
  --version        show version and quit
  
  [1]
