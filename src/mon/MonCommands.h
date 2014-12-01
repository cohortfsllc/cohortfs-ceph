// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

/* no guard; may be included multiple times */

/*
 * Define commands that are reported by the monitor's
 * "get_command_descriptions" command, and parsed by the Python
 * frontend 'ceph' (and perhaps by other frontends, such as a RESTful
 * server). The format is:
 *
 * COMMAND(signature, helpstring, modulename, req perms, availability)
 * where:
 * signature:  describes the command and its parameters (more below)
 * helpstring: displays in CLI help, API help (nice if it refers to
 *	       parameter names from signature, 40-a few hundred chars)
 * modulename: the monitor module or daemon this applies to:
 *	       mds, osd, mon, auth, log, config-key
 * req perms:  required permission in that modulename space to execute command
 *	       this also controls what type of REST command is accepted
 * availability: cli, rest, or both
 *
 * The commands describe themselves completely enough for the separate
 * frontend(s) to be able to accept user input and validate it against
 * the command descriptions, and generate a JSON object that contains
 * key:value mappings of parameter names to validated parameter values.
 *
 * 'signature' is a space-separated list of individual command
 * descriptors; each descriptor is either a literal string, which can
 * contain no spaces or '=' signs or a list of key=val[,key=val...]
 * which also includes no spaces.
 *
 * The key=val form describes a non-literal parameter.	Each will have at
 * least a name= and type=, and each type can have its own type-specific
 * parameters.	The parser is the arbiter of these types and their
 * interpretation.  A few more non-type-specific key=val pairs exist:
 *
 *    req=false marks an optional parameter (default for req is 'true')
 *    n=<n> is a repeat count for how many of this argument must be supplied.
 *	    n=1 is the default.
 *	    n=N is a special case that means "1 or more".
 *
 * A perhaps-incomplete list of types:
 *
 * CephInt: Optional: range=min[|max]
 * CephFloat: Optional range
 * CephString: optional badchars
 * CephSocketpath: validation involves "is it S_ISSOCK"
 * CephIPAddr: v4 or v6 addr with optional port, syntax validated
 * CephEntityAddr: CephIPAddr + optional '/nonce'
 * CephObjectname: Another plainold string
 * CephName: daemon name, '*' or '<type>.<id>' (id must be int for type osd)
 * CephOsdName: osd name, '*' or '<id> or 'osd.<id>' (id must be int)
 * CephChoices: strings="foo|bar" means this param can be either
 * CephFilepath: openable file
 * CephFragment: cephfs 'fragID': val/bits, val in hex 0xnnn, bits in dec
 * CephUUID: uuid in text matching Python uuid.UUID()
 * CephPrefix: special type assigned to literals
 *
 * Example:
 *
 * COMMAND("auth add " \
 *	   "name=entity,type=CephString "	      \
 *	   "name=caps,type=CephString,n=N,req=false", \
 *	   "add auth info for <name> from input file, or random key " \
 *	   "if no input given, and/or any caps specified in the command")
 *
 * defines a command "auth add" that takes a required argument "entity"
 * of type "CephString", and from 1 to N arguments named "caps" of type
 * CephString, at least one of which is required.  The front end will
 * validate user input against this description.  Let's say the user
 * enters auth add client.admin 'mon rwx' 'osd *'.  The result will be a
 * JSON object like {"prefix":"auth add", "entity":"client.admin",
 * "caps":["mon rwx", "osd *"]}.
 * Note that
 *	- string literals are accumulated into 'prefix'
 *	- n=1 descriptors are given normal string or int object values
 *	- n=N descriptors are given array values
 *
 * NOTE: be careful with spaces.  Each descriptor must be separated by
 * one space, no other characters, so if you split lines as above, be
 * sure to close and reopen the quotes, and be careful to include the '
 * separating spaces in the quoted string.
 *
 * The monitor marshals this JSON into a std::map<string, cmd_vartype>
* where cmd_vartype is a boost::variant type-enforcing discriminated
* type, so the monitor is expected to know the type of each argument.
* See cmdparse.cc/h for more details.
*/

/*
 * auth commands AuthMonitor.cc
 */

COMMAND("auth export name=entity,type=CephString,req=false", \
	"write keyring for requested entity, or master keyring if none given", \
	"auth", "rx", "cli,rest")
COMMAND("auth get name=entity,type=CephString", \
	"write keyring file with requested key", "auth", "rx", "cli,rest")
COMMAND("auth get-key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth print-key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth print_key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth list", "list authentication state", "auth", "rx", "cli,rest")
COMMAND("auth import", "auth import: read keyring file from -i <file>", \
	"auth", "rwx", "cli,rest")
COMMAND("auth add " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"add auth info for <entity> from input file, or random key if no input given, and/or any caps specified in the command",
	"auth", "rwx", "cli,rest")
COMMAND("auth get-or-create-key " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"get, or add, key for <name> from system/caps pairs specified in the command.  If key already exists, any given caps must match the existing caps for that key.", \
	"auth", "rwx", "cli,rest")
COMMAND("auth get-or-create " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"add auth info for <entity> from input file, or random key if no input given, and/or any caps specified in the command", \
	"auth", "rwx", "cli,rest")
COMMAND("auth caps " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N", \
	"update caps for <name> from caps specified in the command", \
	"auth", "rwx", "cli,rest")
COMMAND("auth del " \
	"name=entity,type=CephString", \
	"delete all caps for <name>", \
	"auth", "rwx", "cli,rest")

/*
 * Monitor commands (Monitor.cc)
 */
COMMAND("compact", "cause compaction of monitor's leveldb storage", \
	"mon", "rw", "cli,rest")
COMMAND("scrub", "scrub the monitor stores", "mon", "rw", "cli,rest")
COMMAND("fsid", "show cluster FSID/UUID", "mon", "r", "cli,rest")
COMMAND("log name=logtext,type=CephString,n=N", \
	"log supplied text to the monitor log", "mon", "rw", "cli,rest")
COMMAND("injectargs " \
	"name=injected_args,type=CephString,n=N", \
	"inject config arguments into monitor", "mon", "rw", "cli,rest")
COMMAND("status", "show cluster status", "mon", "r", "cli,rest")
COMMAND("health name=detail,type=CephChoices,strings=detail,req=false", \
	"show cluster health", "mon", "r", "cli,rest")
COMMAND("report name=tags,type=CephString,n=N,req=false", \
	"report full status of cluster, optional title tag strings", \
	"mon", "r", "cli,rest")
COMMAND("quorum_status", "report status of monitor quorum", \
	"mon", "r", "cli,rest")
COMMAND("mon_status", "report status of monitors", "mon", "r", "cli,rest")
COMMAND("sync force " \
	"name=validate1,type=CephChoices,strings=--yes-i-really-mean-it,req=false " \
	"name=validate2,type=CephChoices,strings=--i-know-what-i-am-doing,req=false", \
	"force sync of and clear monitor store", "mon", "rw", "cli,rest")
COMMAND("heap " \
	"name=heapcmd,type=CephChoices,strings=dump|start_profiler|stop_profiler|release|stats", \
	"show heap usage info (available only if compiled with tcmalloc)", \
	"mon", "rw", "cli,rest")
COMMAND("quorum name=quorumcmd,type=CephChoices,strings=enter|exit,n=1", \
	"enter or exit quorum", "mon", "rw", "cli,rest")
COMMAND("tell " \
	"name=target,type=CephName " \
	"name=args,type=CephString,n=N", \
	"send a command to a specific daemon", "mon", "rw", "cli,rest")

/*
 * MDS commands (MDSMonitor.cc)
 */

COMMAND("mds stat", "show MDS status", "mds", "r", "cli,rest")
COMMAND("mds dump "
	"name=epoch,type=CephInt,req=false,range=0", \
	"dump info, optionally from epoch", "mds", "r", "cli,rest")
COMMAND("mds getmap " \
	"name=epoch,type=CephInt,req=false,range=0", \
	"get MDS map, optionally from epoch", "mds", "r", "cli,rest")
COMMAND("mds tell " \
	"name=who,type=CephString " \
	"name=args,type=CephString,n=N", \
	"send command to particular mds", "mds", "rw", "cli,rest")
COMMAND("mds compat show", "show mds compatibility settings", \
	"mds", "r", "cli,rest")
COMMAND("mds stop name=who,type=CephString", "stop mds", \
	"mds", "rw", "cli,rest")
COMMAND("mds deactivate name=who,type=CephString", "stop mds", \
	"mds", "rw", "cli,rest")
COMMAND("mds set_max_mds " \
	"name=maxmds,type=CephInt,range=0", \
	"set max MDS index", "mds", "rw", "cli,rest")
COMMAND("mds set " \
	"name=var,type=CephChoices,strings=max_mds|max_file_size|inline_data " \
	"name=val,type=CephString "					\
	"name=confirm,type=CephString,req=false",			\
	"set mds parameter <var> to <val>", "mds", "rw", "cli,rest")
COMMAND("mds setmap " \
	"name=epoch,type=CephInt,range=0", \
	"set mds map; must supply correct epoch number", "mds", "rw", "cli,rest")
// arbitrary limit 0-20 below; worth standing on head to make it
// relate to actual state definitions?
// #include "include/ceph_fs.h"
COMMAND("mds set_state " \
	"name=gid,type=CephInt,range=0 " \
	"name=state,type=CephInt,range=0|20", \
	"set mds state of <gid> to <numeric-state>", "mds", "rw", "cli,rest")
COMMAND("mds fail name=who,type=CephString", \
	"force mds to status failed", "mds", "rw", "cli,rest")
COMMAND("mds rm " \
	"name=gid,type=CephInt,range=0 " \
	"name=who,type=CephName", \
	"remove nonactive mds", "mds", "rw", "cli,rest")
COMMAND("mds rmfailed name=who,type=CephInt,range=0", "remove failed mds", \
	"mds", "rw", "cli,rest")
COMMAND("mds cluster_down", "take MDS cluster down", "mds", "rw", "cli,rest")
COMMAND("mds cluster_up", "bring MDS cluster up", "mds", "rw", "cli,rest")
COMMAND("mds compat rm_compat " \
	"name=feature,type=CephInt,range=0", \
	"remove compatible feature", "mds", "rw", "cli,rest")
COMMAND("mds compat rm_incompat " \
	"name=feature,type=CephInt,range=0", \
	"remove incompatible feature", "mds", "rw", "cli,rest")
COMMAND("mds newfs " \
	"name=metadata,type=CephString " \
	"name=data,type=CephString " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"make new filesystom", \
	"mds", "rw", "cli,rest")
/*
 * Monmap commands
 */
COMMAND("mon dump " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"dump formatted monmap (optionally from epoch)", \
	"mon", "r", "cli,rest")
COMMAND("mon stat", "summarize monitor status", "mon", "r", "cli,rest")
COMMAND("mon getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get monmap", "mon", "r", "cli,rest")
COMMAND("mon add " \
	"name=name,type=CephString " \
	"name=addr,type=CephIPAddr", \
	"add new monitor named <name> at <addr>", "mon", "rw", "cli,rest")
COMMAND("mon remove " \
	"name=name,type=CephString", \
	"remove monitor named <name>", "mon", "rw", "cli,rest")


/*
 * OSD commands
 */
COMMAND("osd stat", "print summary of OSD map", "osd", "r", "cli,rest")
COMMAND("osd dump " \
	"name=epoch,type=CephInt,range=0,req=false",
	"print summary of OSD map", "osd", "r", "cli,rest")
COMMAND("osd ls " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"show all OSD ids", "osd", "r", "cli,rest")
COMMAND("osd getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get OSD map", "osd", "r", "cli,rest")
COMMAND("osd getmaxosd", "show largest OSD id", "osd", "r", "cli,rest")
COMMAND("osd metadata " \
	"name=id,type=CephInt,range=0", \
	"fetch metadata for osd <id>", \
	"osd", "r", "cli,rest")
COMMAND("osd blacklist ls", "show blacklisted clients", "osd", "r", "cli,rest")
COMMAND("osd setmaxosd " \
	"name=newmax,type=CephInt,range=0", \
	"set new maximum osd value", "osd", "rw", "cli,rest")
COMMAND("osd pause", "pause osd", "osd", "rw", "cli,rest")
COMMAND("osd unpause", "unpause osd", "osd", "rw", "cli,rest")
COMMAND("osd set " \
	"name=key,type=CephChoices,strings=pause|noup|nodown|noout|noin", \
	"set <key>", "osd", "rw", "cli,rest")
COMMAND("osd unset " \
	"name=key,type=CephChoices,strings=pause|noup|nodown|noout|noin", \
	"unset <key>", "osd", "rw", "cli,rest")
COMMAND("osd down " \
	"type=CephString,name=ids,n=N", \
	"set osd(s) <id> [<id>...] down", "osd", "rw", "cli,rest")
COMMAND("osd out " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] out", "osd", "rw", "cli,rest")
COMMAND("osd in " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] in", "osd", "rw", "cli,rest")
COMMAND("osd rm " \
	"name=ids,type=CephString,n=N", \
	"remove osd(s) <id> [<id>...] in", "osd", "rw", "cli,rest")
COMMAND("osd reweight " \
	"name=id,type=CephInt,range=0 " \
	"type=CephFloat,name=weight,range=0.0|1.0", \
	"reweight osd to 0.0 < <weight> < 1.0", "osd", "rw", "cli,rest")
COMMAND("osd lost " \
	"name=id,type=CephInt,range=0 " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"mark osd as permanently lost. THIS DESTROYS DATA IF NO MORE REPLICAS EXIST, BE CAREFUL", \
	"osd", "rw", "cli,rest")
COMMAND("osd create " \
	"name=uuid,type=CephUUID,req=false", \
	"create new osd (with optional UUID)", "osd", "rw", "cli,rest")
COMMAND("osd blacklist " \
	"name=blacklistop,type=CephChoices,strings=add|rm " \
	"name=addr,type=CephEntityAddr " \
	"name=expire,type=CephFloat,range=0.0,req=false", \
	"add (optionally until <expire> seconds from now) or remove <addr> from blacklist", \
	"osd", "rw", "cli,rest")
COMMAND("osd volume create "					 \
	"name=volumeName,type=CephString "			 \
	"name=stripeUnit,type=CephInt,req=false "		 \
	"name=erasurePluginName,type=CephString,req=false "	 \
	"name=erasureParams,type=CephString,req=false "		 \
	"name=placeCode,type=CephString,req=false "		 \
	"name=placeSymbols,type=CephString,req=false ",		 \
	"Create a new volume",					 \
	"osd", "rw", "cli,rest")
COMMAND("osd volume remove "		   \
	"name=volumeName,type=CephString", \
	"Remove a volume",		   \
	"osd", "rw", "cli,rest")
COMMAND("osd volume list "				\
	"name=pattern,type=CephString,req=false",	\
	"list volumes",					\
	"osd", "r", "cli,rest")

/*
 * mon/ConfigKeyService.cc
 */

COMMAND("config-key get " \
	"name=key,type=CephString", \
	"get <key>", "config-key", "r", "cli,rest")
COMMAND("config-key put " \
	"name=key,type=CephString " \
	"name=val,type=CephString,req=false", \
	"put <key>, value <val>", "config-key", "rw", "cli,rest")
COMMAND("config-key del " \
	"name=key,type=CephString", \
	"delete <key>", "config-key", "rw", "cli,rest")
COMMAND("config-key exists " \
	"name=key,type=CephString", \
	"check for <key>'s existence", "config-key", "r", "cli,rest")
COMMAND("config-key list ", "list keys", "config-key", "r", "cli,rest")
