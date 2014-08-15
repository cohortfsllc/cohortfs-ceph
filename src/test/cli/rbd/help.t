  $ rbd --help
  usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...
  where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:
    (ls | list)                                 [pool-name] list rbd images
    info <image-name>                           show information about image size,
                                                striping, etc.
    create [--order <bits>] --size <MB> <name>  create an empty image
    resize --size <MB> <image-name>             resize (expand or contract) image
    rm <image-name>                             delete an image
    export <image-name> <path>                  export image to file
                                                "-" for stdout
    import <path> <image-name>                  import image from file
                                                (dest defaults
                                                 as the filename part of file)
                                                "-" for stdin
    (cp | copy) <src> <dest>                    copy src image to dest
    (mv | rename) <src> <dest>                  rename src image to dest
    watch <image-name>                          watch events on image
    map <image-name>                            map image to a block device
                                                using the kernel
    unmap <device>                              unmap a rbd device that was
                                                mapped by the kernel
    showmapped                                  show the rbd images mapped
                                                by the kernel
    lock list <image-name>                      show locks held on an image
    lock add <image-name> <id> [--shared <tag>] take a lock called id on an image
    lock remove <image-name> <id> <locker>      release a lock on an image
    bench-write <image-name>                    simple write benchmark
                   --io-size <bytes>              write size
                   --io-threads <num>             ios in flight
                   --io-total <bytes>             total bytes to write
                   --io-pattern <seq|rand>        write pattern

  <image-name> are [pool/]name, or you may specify
  individual pieces of names with -p/--pool and/or --image.

  Other input options:
    -p, --pool <pool>                  source pool name
    --image <image-name>               image name
    --dest <image-name>                destination [pool and] image name
    --dest-pool <name>                 destination pool name
    --path <path-name>                 path name for import/export
    --size <size in MB>                size of image for create and resize
    --order <bits>                     the object size in bits; object size will be
                                       (1 << order) bytes. Default is 22 (4 MB).
    --image-format <format-number>     format to use when creating an image
                                       format 1 is the original format (default)
                                       format 2 supports cloning
    --id <username>                    rados user (without 'client.'prefix) to
                                       authenticate as
    --keyfile <path>                   file containing secret key for use with cephx
    --shared <tag>                     take a shared (rather than exclusive) lock
    --format <output-format>           output format (default: plain, json, xml)
    --pretty-format                    make json or xml output more readable
    --no-settle                        do not wait for udevadm to settle on map/unmap
    --no-progress                      do not show progress for long-running commands
    -o, --options <map-options>        options to use when mapping an image
    --read-only                        set device readonly when mapping image
    --allow-shrink                     allow shrinking of an image when resizing
