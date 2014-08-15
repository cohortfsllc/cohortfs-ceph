ls on empty pool never containing images
========================================
  $ rados -p rbd rm rbd_directory >/dev/null 2>&1 || true
  $ rbd ls
  $ rbd ls --format json
  [] (no-eol)
  $ rbd ls --format xml
  <images></images> (no-eol)

create
=======
  $ rbd create -s 1024 foo
  $ rbd create -s 512 --image-format 2 bar
  $ rbd create -s 2048 --image-format 2 baz
  $ rbd create -s 1 quux


lock
====
  $ rbd lock add quux id
  $ rbd lock add baz id1 --shared tag
  $ rbd lock add baz id2 --shared tag
  $ rbd lock add baz id3 --shared tag

test formatting
===============
TODO: figure out why .* does not match the block_name_prefix line in rbd info.
For now, use a more inclusive regex.
  $ rbd info foo
  rbd image 'foo':
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info foo --format json | python -mjson.tool
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1,
      "name": "foo",
      "object_size": 4194304,
      "objects": 256,
      "order": 22,
      "size": 1073741824
  }
The version of xml_pp included in ubuntu precise always prints a 'warning'
whenever it is run. grep -v to ignore it, but still work on other distros.
  $ rbd info foo --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>foo</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd info bar
  rbd image 'bar':
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering (esc)
  $ rbd info bar --format json | python -mjson.tool
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering",
          "striping"
      ],
      "format": 2,
      "name": "bar",
      "object_size": 4194304,
      "objects": 256,
      "order": 22,
      "size": 1073741824
  }
  $ rbd info bar --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>bar</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>striping</feature>
    </features>
  </image>
  $ rbd info baz
  rbd image 'baz':
  \tsize 2048 MB in 512 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering (esc)
  $ rbd info baz --format json | python -mjson.tool
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering",
          "striping"
      ],
      "format": 2,
      "name": "baz",
      "object_size": 4194304,
      "objects": 512,
      "order": 22,
      "size": 2147483648
  }
  $ rbd info baz --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>baz</name>
    <size>2147483648</size>
    <objects>512</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>striping</feature>
    </features>
  </image>
  $ rbd info quux
  rbd image 'quux':
  \tsize 1024 kB in 1 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info quux --format json | python -mjson.tool
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1,
      "name": "quux",
      "object_size": 4194304,
      "objects": 1,
      "order": 22,
      "size": 1048576
  }
  $ rbd info quux --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>quux</name>
    <size>1048576</size>
    <objects>1</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd list
  foo
  quux
  bar
  baz
  $ rbd list --format json | python -mjson.tool
  [
      "foo",
      "quux",
      "bar",
      "baz"
  ]
  $ rbd list --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <images>
    <name>foo</name>
    <name>quux</name>
    <name>bar</name>
    <name>baz</name>
  </images>
  $ rbd lock list foo
  $ rbd lock list foo --format json | python -mjson.tool
  {}
  $ rbd lock list foo --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks></locks>
  $ rbd lock list quux
  There is 1 exclusive lock on this image.
  Locker*ID*Address* (glob)
  client.* id * (glob)
  $ rbd lock list quux --format json | python -mjson.tool
  {
      "id": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }
  }
  $ rbd lock list quux --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks>
    <id>
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id>
  </locks>
  $ rbd lock list baz
  There are 3 shared locks on this image.
  Lock tag: tag
  Locker*ID*Address* (glob)
  client.*id[123].* (re)
  client.*id[123].* (re)
  client.*id[123].* (re)
  $ rbd lock list baz --format json | python -mjson.tool
  {
      "id1": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      },
      "id2": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      },
      "id3": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }
  }
  $ rbd lock list baz --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks>
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
  </locks>

# cleanup
  $ rbd rm foo 2> /dev/null
  $ rbd rm bar 2> /dev/null
  $ rbd rm quux 2> /dev/null
  $ rbd rm baz 2> /dev/null
