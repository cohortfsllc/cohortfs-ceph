  $ monmaptool --create --add foo 2.3.4.5:6789 mymonmap
  monmaptool: monmap file mymonmap
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} can't open mymonmap: error 2: No such file or directory (re)
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 1 to mymonmap (1 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: mymonmap exists, --clobber to overwrite
  [255]

# hasn't changed yet
  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:6789/0 mon.foo

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]

  $ monmaptool --create --clobber mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 1 to mymonmap (0 monitors)

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" != "$NEW_FSID" ]

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
