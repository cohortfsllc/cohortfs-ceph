#!@PYTHON_EXECUTABLE@
#
# Processed in Makefile to add python #! line and version variable
#
#
# vim: ts=4 sw=4 smarttab expandtab

"""
ceph.in becomes ceph, the command-line management tool for Ceph clusters.
This is a replacement for tools/ceph.cc and tools/common.cc.

Copyright (C) 2013 Inktank Storage, Inc.

This is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public
License version 2, as published by the Free Software
Foundation.  See file COPYING.
"""

import os
import sys

# Make life easier on developers:
# If in src/, and .libs and pybind exist here, assume we're running
# from a Ceph source dir and tweak PYTHONPATH and LD_LIBRARY_PATH
# to use local files

MYPATH = os.path.abspath(__file__)
MYDIR = os.path.dirname(MYPATH)
DEVMODEMSG = '*** DEVELOPER MODE: setting PYTHONPATH and LD_LIBRARY_PATH'

if MYDIR.endswith('src') and \
   os.path.exists(os.path.join(MYDIR, '.libs')) and \
   os.path.exists(os.path.join(MYDIR, 'pybind')):
    MYLIBPATH = os.path.join(MYDIR, '.libs')
    if 'LD_LIBRARY_PATH' in os.environ:
        if MYLIBPATH not in os.environ['LD_LIBRARY_PATH']:
            os.environ['LD_LIBRARY_PATH'] += ':' + MYLIBPATH
            print >> sys.stderr, DEVMODEMSG
            os.execvp('python', ['python'] + sys.argv)
    else:
        os.environ['LD_LIBRARY_PATH'] = MYLIBPATH
        print >> sys.stderr, DEVMODEMSG
        os.execvp('python', ['python'] + sys.argv)
    sys.path.insert(0, os.path.join(MYDIR, 'pybind'))

import argparse
import errno
import json
import rados
import signal
import socket
import string
import struct
import subprocess

from ceph_argparse import *

# just a couple of globals

verbose = False
cluster_handle = None

############################################################################

def osdids():
    ret, outbuf, outs = json_command(cluster_handle, prefix='osd ls')
    if ret:
        raise RuntimeError('Can\'t contact mon for osd list')
    return [i for i in outbuf.split('\n') if i != '']

def monids():
    ret, outbuf, outs = json_command(cluster_handle, prefix='mon dump',
                                     argdict={'format':'json'})
    if ret:
        raise RuntimeError('Can\'t contact mon for mon list')
    d = json.loads(outbuf)
    return [m['name'] for m in d['mons']]

def mdsids():
    ret, outbuf, outs = json_command(cluster_handle, prefix='mds dump',
                                     argdict={'format':'json'})
    if ret:
        raise RuntimeError('Can\'t contact mon for mds list')
    d = json.loads(outbuf)
    l = []
    infodict = d['info']
    for mdsdict in infodict.values():
        l.append(mdsdict['name'])
    return l

def parse_cmdargs(args=None, target=''):
    # alias: let the line-wrapping be sane
    AP = argparse.ArgumentParser

    # format our own help
    parser = AP(description='Ceph administration tool', add_help=False)

    parser.add_argument('--completion', action='store_true',
                        help=argparse.SUPPRESS)

    parser.add_argument('-h', '--help', help='request mon help',
                        action='store_true')
    parser.add_argument('--help-all', help='request help for all daemons',
                        action='store_true')

    parser.add_argument('-c', '--conf', dest='cephconf',
                        help='ceph configuration file')
    parser.add_argument('-i', '--in-file', dest='input_file',
                        help='input file')
    parser.add_argument('-o', '--out-file', dest='output_file',
                        help='output file')

    parser.add_argument('--id', '--user', dest='client_id',
                        help='client id for authentication')
    parser.add_argument('--name', '-n', dest='client_name',
                        help='client name for authentication')
    parser.add_argument('--cluster', help='cluster name')

    parser.add_argument('--admin-daemon', dest='admin_socket',
                        help='submit admin-socket commands (\"help\" for help')

    parser.add_argument('-s', '--status', action='store_true',
                        help='show cluster status')

    parser.add_argument('-w', '--watch', action='store_true',
                        help='watch live cluster changes')
    parser.add_argument('--watch-debug', action='store_true',
                        help='watch debug events')
    parser.add_argument('--watch-info', action='store_true',
                        help='watch info events')
    parser.add_argument('--watch-sec', action='store_true',
                        help='watch security events')
    parser.add_argument('--watch-warn', action='store_true',
                        help='watch warn events')
    parser.add_argument('--watch-error', action='store_true',
                        help='watch error events')

    parser.add_argument('--version', '-v', action="store_true", help="display version")
    parser.add_argument('--verbose', action="store_true", help="make verbose")
    parser.add_argument('--concise', dest='verbose', action="store_false",
                        help="make less verbose")

    parser.add_argument('-f', '--format', choices=['json', 'json-pretty',
                        'xml', 'xml-pretty', 'plain'], dest='output_format')
    # for pg dump_stuck
    parser.add_argument('--threshold', type=int, help='number of seconds for a pg to be considered stuck for pg dump_stuck')

    # returns a Namespace with the parsed args, and a list of all extras
    parsed_args, extras = parser.parse_known_args(args)

    return parser, parsed_args, extras

def do_help(parser, args, help_all = False):
    """
    Print basic parser help
    If the cluster is available:
        get and print monitor help;
        if help_all, print help for daemon commands as well
    """

    def help_for_target(target, partial=None):
        ret, outbuf, outs = json_command(cluster_handle, target=target,
                                         prefix='get_command_descriptions',
                                         timeout=10)
        if ret:
            print >> sys.stderr, \
                "couldn't get command descriptions for {0}: {1}".\
                format(target, outs)
        else:
            sys.stdout.write(format_help(parse_json_funcsigs(outbuf), partial))

    parser.print_help()
    print '\n'
    if (cluster_handle):
        help_for_target(target=('mon', ''), partial=' '.join(args))

    if help_all and cluster_handle:
        # try/except in case there are no daemons of that type
        try:
            firstosd = osdids()[0]
            print '\nOSD.{0} tell commands and pg pgid commands:\n\n'.\
                format(firstosd)
            help_for_target(target=('osd', osdids()[0]))

            print '\nOSD daemon commands:\n\n'
            sys.stdout.write(format_help(parse_json_funcsigs(admin_socket(ceph_conf('admin_socket', 'osd.' + firstosd), ['get_command_descriptions']))))
        except:
            pass

        try:
            firstmon = monids()[0]
            print '\nmon.{0} daemon commands:\n\n'.format(firstmon)
            sys.stdout.write(format_help(parse_json_funcsigs(admin_socket(ceph_conf('admin_socket', 'mon.' + firstmon), ['get_command_descriptions']))))
        except:
            pass

        try:
            firstmds = mdsids()[0]
            print '\nmds.{0} daemon commands:\n\n'.format(firstmds)
            sys.stdout.write(format_help(parse_json_funcsigs(admin_socket(ceph_conf('admin_socket', 'mds.' + firstmds), ['get_command_descriptions']))))
        except:
            pass

    return 0


def descsort(sh1, sh2):
    """
    sort descriptors by prefixes, defined as the concatenation of all simple
    strings in the descriptor; this works out to just the leading strings.
    """
    return cmp(concise_sig(sh1['sig']), concise_sig(sh2['sig']))


DONTSPLIT = string.letters + '{[<>]}'

def wrap(s, width, indent):
    """
    generator to transform s into a sequence of strings width or shorter,
    for wrapping text to a specific column width.
    Attempt to break on anything but DONTSPLIT characters.
    indent is amount to indent 2nd-through-nth lines.

    so "long string long string long string" width=11 indent=1 becomes
    'long string', ' long string', ' long string' so that it can be printed
    as
    long string
     long string
     long string

    Consumes s.
    """
    result = ''
    leader = ''
    while len(s):

        if (len(s) <= width):
            # no splitting; just possibly indent
            result = leader + s
            s = ''
            yield result

        else:
            splitpos = width
            while (splitpos > 0) and (s[splitpos-1] in DONTSPLIT):
                splitpos -= 1

            if splitpos == 0:
                splitpos = width

            if result:
                # prior result means we're mid-iteration, indent
                result = leader
            else:
                # first time, set leader and width for next
                leader = ' ' * indent
                width -= 1      # for subsequent space additions

            # remove any leading spaces in this chunk of s
            result += s[:splitpos].lstrip()
            s = s[splitpos:]

            yield result

    raise StopIteration

def format_help(cmddict, partial=None):
    """
    Formats all the cmdsigs and helptexts from cmddict into a sorted-by-
    cmdsig 2-column display, with each column wrapped and indented to
    fit into 40 characters.
    """

    fullusage = ''
    for cmd in sorted(cmddict.itervalues(), cmp=descsort):

        if not cmd['helptext']:
            continue
        concise = concise_sig(cmd['sig'])
        if partial and not concise.startswith(partial):
            continue
        siglines = [l for l in wrap(concise, 40, 1)]
        helplines = [l for l in wrap(cmd['helptext'], 39, 1)]

        # make lists the same length
        maxlen = max(len(siglines), len(helplines))
        siglines.extend([''] * (maxlen - len(siglines)))
        helplines.extend([''] * (maxlen - len(helplines)))

        # so we can zip them for output
        for (s, h) in zip(siglines, helplines):
            fullusage += '{0:40s} {1}\n'.format(s, h)

    return fullusage

def admin_socket(asok_path, cmd):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(asok_path)
        sock.sendall(' '.join(cmd) + '\0')

        len_str = sock.recv(4)
        l, = struct.unpack(">I", len_str)
        ret = ''

        got = 0
        while got < l:
            bit = sock.recv(l - got)
            ret += bit
            got += len(bit)

    except Exception as e:
        raise RuntimeError('exception: {0}'.format(e))

    return ret


def ceph_conf(field, name):
    p = subprocess.Popen(
        args=[
            'ceph-conf',
	    '--show-config-value',
            field,
            '-n',
            name,
            ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    outdata, errdata = p.communicate()
    if (len(errdata)):
        raise RuntimeError('unable to get conf option %s for %s: %s' % (field, name, errdata))
    return outdata.rstrip()

def new_style_command(parsed_args, cmdargs, target, sigdict, inbuf, verbose):
    """
    Do new-style command dance.
    target: daemon to receive command: mon (any) or osd.N
    sigdict - the parsed output from the new monitor describing commands
    inbuf - any -i input file data
    verbose - bool
    """
    if verbose:
        for cmdtag in sorted(sigdict.keys()):
            cmd = sigdict[cmdtag]
            sig = cmd['sig']
            print '{0}: {1}'.format(cmdtag, concise_sig(sig))

    got_command = False

    if not got_command:
        if cmdargs:
            # Validate input args against list of sigs
            valid_dict = validate_command(parsed_args, sigdict, cmdargs,
                                          verbose)
            if valid_dict:
                got_command = True
            else:
                return -errno.EINVAL, '', 'invalid command'
        else:
            # do the command-interpreter looping
            # for raw_input to do readline cmd editing
            import readline
            while True:
                interactive_input = raw_input('ceph> ')
                if interactive_input in ['q', 'quit', 'Q']:
                    return 0, '', ''
                cmdargs = parse_cmdargs(interactive_input.split())[2]
                target = find_cmd_target(cmdargs)
                valid_dict = validate_command(parsed_args, sigdict, cmdargs)
                if valid_dict:
                    if verbose:
                        print >> sys.stderr, "Submitting command ", valid_dict
                    ret, outbuf, outs = json_command(cluster_handle,
                                                     target=target,
                                                     argdict=valid_dict)
                    if ret:
                        sys.stderr.write('Error {0}: {1}'.format(ret, outs))
                        return ret, '', outs
                else:
                    print "invalid command"

    if verbose:
        print >> sys.stderr, "Submitting command ", valid_dict
    return json_command(cluster_handle, target=target, argdict=valid_dict,
                        inbuf=inbuf)

OSD_TELL_MATCH = { \
    'sig': ['tell', {'name':'target','type':'CephName'}], \
    'matchnum': 2, \
    'return_key': 'target', \
}
PGID_MATCH = { \
    'sig': ['pg', {'name':'pgid','type':'CephPgid'}], \
    'matchnum': 2, \
    'return_key': 'pgid', \
}

def find_cmd_target(childargs):
    """
    Using a minimal validation, figure out whether the command
    should be sent to a monitor or an osd.  We do this before even
    asking for the 'real' set of command signatures, so we can ask the
    right daemon.
    Returns ('osd', osdid), ('pg', pgid), or ('mon', '')
    """
    sig = parse_funcsig(['tell', {'name':'target','type':'CephName'}])
    valid_dict = validate(childargs, sig, partial=True);
    if len(valid_dict) == 2:
        name = CephName()
        name.valid(valid_dict['target'])
        return name.nametype, name.nameid

    sig = parse_funcsig(['pg', {'name':'pgid','type':'CephPgid'}])
    valid_dict = validate(childargs, sig, partial=True);
    if len(valid_dict) == 2:
        return 'pg', valid_dict['pgid']

    return 'mon', ''

def complete(sigdict, args, target):
    """
    Command completion.  Match as much of [args] as possible,
    and print every possible match separated by newlines.
    Return exitcode.
    """
    # XXX this looks a lot like the front of validate_command().  Refactor?

    complete_verbose = 'COMPVERBOSE' in os.environ

    # Repulsive hack to handle tell: lop off 'tell' and target
    # and validate the rest of the command.  'target' is already
    # determined in our callers, so it's ok to remove it here.
    if len(args) and args[0] == 'tell':
        args = args[2:]
    # look for best match, accumulate possibles in bestcmds
    # (so we can maybe give a more-useful error message)
    best_match_cnt = 0
    bestcmds = []
    for cmdtag, cmd in sigdict.iteritems():
        sig = cmd['sig']
        matched = matchnum(args, sig, partial=True)
        if (matched > best_match_cnt):
            if complete_verbose:
                print >> sys.stderr, \
                    "better match: {0} > {1}: {2}:{3} ".format(matched,
                                  best_match_cnt, cmdtag, concise_sig(sig))
            best_match_cnt = matched
            bestcmds = [{cmdtag:cmd}]
        elif matched == best_match_cnt:
            if complete_verbose:
                print >> sys.stderr, \
                    "equal match: {0} > {1}: {2}:{3} ".format(matched,
                                  best_match_cnt, cmdtag, concise_sig(sig))
            bestcmds.append({cmdtag:cmd})

    # look through all matching sigs
    comps = []
    for cmddict in bestcmds:
        for cmd in cmddict.itervalues():
            sig = cmd['sig']
            # either:
            #   we match everything fully, so we want the next desc, or
            #   we match more partially, so we want the partial match
            fullindex = matchnum(args, sig, partial=False) - 1
            partindex = matchnum(args, sig, partial=True) - 1
            if complete_verbose:
                print >> sys.stderr, '{}: f {} p {} len {}'.format(sig, fullindex, partindex, len(sig))
            if fullindex == partindex and fullindex + 1 < len(sig):
                d = sig[fullindex + 1]
            else:
                d = sig[partindex]
            comps.append(str(d))
    if complete_verbose:
        print >> sys.stderr, '\n'.join(comps)
    print '\n'.join(comps)

    return 0

###
# main
###

def main():

    parser, parsed_args, childargs = parse_cmdargs()

    if parsed_args.version:
        print 'ceph version {0} ({1})'.format('@CEPH_GIT_NICE_VER@', '@CEPH_GIT_VER@')
        return 0

    global verbose
    verbose = parsed_args.verbose

    # pass on --id, --name, --conf
    name = 'client.admin'
    if parsed_args.client_id:
        name = 'client.' + parsed_args.client_id
    if parsed_args.client_name:
        name = parsed_args.client_name

    # default '' means default conf search
    conffile = ''
    if parsed_args.cephconf:
        conffile = parsed_args.cephconf
    # For now, --admin-daemon is handled as usual.  Try it
    # first in case we can't connect() to the cluster
    if parsed_args.admin_socket:
        try:
            print admin_socket(parsed_args.admin_socket, childargs)
        except Exception as e:
            print >> sys.stderr, 'admin_socket: {0}'.format(e)
        return 0

    if len(childargs) > 0 and childargs[0] == "daemon":
        if len(childargs) > 2:
            if childargs[1].find('/') >= 0:
                try:
                    print admin_socket(childargs[1], childargs[2:])
                except Exception as e:
                    print >> sys.stderr, 'admin_socket: {0}'.format(e)
                return 0
            else:
                # try resolve daemon name
                path = ceph_conf('admin_socket', childargs[1])
                try:
                    print admin_socket(path, childargs[2:])
                except Exception as e:
                    print >> sys.stderr, 'admin_socket: {0}'.format(e)
                return 0
        else:
            print >> sys.stderr, 'Daemon requires at least 2 arguments'
            return 1

    # handle any 'generic' ceph arguments that we didn't parse here
    global cluster_handle

    # rados.Rados() will call rados_create2, and then read the conf file,
    # and then set the keys from the dict.  So we must do these
    # "pre-file defaults" first (see common_preinit in librados)
    conf_defaults = {
        'log_to_stderr':'true',
        'err_to_stderr':'true',
        'log_flush_on_exit':'true',
    }

    clustername = 'ceph'
    if parsed_args.cluster:
        clustername = parsed_args.cluster

    cluster_handle = rados.Rados(name=name, clustername=clustername,
                                 conf_defaults=conf_defaults, conffile=conffile)

    retargs = cluster_handle.conf_parse_argv(childargs)
    #tmp = childargs
    childargs = retargs
    if not childargs:
        childargs = []

    # -- means "stop parsing args", but we don't want to see it either
    if '--' in childargs:
        childargs.remove('--')

    # special deprecation warning for 'ceph <type> tell'
    # someday 'mds' will be here too
    if len(childargs) >= 2 and \
        childargs[0] in ['mon', 'osd'] and \
        childargs[1] == 'tell':
        print >> sys.stderr, '"{0} tell" is deprecated; try "tell {0}.<id>" instead (id can be "*") '.format(childargs[0])
        return 1

    try:
        cluster_handle.connect()
    except KeyboardInterrupt:
        print >> sys.stderr, 'Cluster connection aborted'
        return 1
    except Exception as e:
        print >> sys.stderr, 'Error connecting to cluster: {0}'.\
            format(e.__class__.__name__)
        return 1

    if parsed_args.help or parsed_args.help_all:
        return do_help(parser, childargs, parsed_args.help_all)

    # implement -w/--watch_*
    # This is ugly, but Namespace() isn't quite rich enough.
    level = ''
    for k,v in parsed_args._get_kwargs():
        if k.startswith('watch') and v:
            if k == 'watch':
                level = 'info'
            else:
                level = k.replace('watch_', '')
    if level:

        # an awfully simple callback
        def watch_cb(arg, line, who, stamp_sec, stamp_nsec, seq, level, msg):
            print line
            sys.stdout.flush()

        # first do a ceph status
        ret, outbuf, outs = json_command(cluster_handle, prefix='status')
        if ret == -errno.EINVAL:
            # try old mon
            ret, outbuf, outs = send_command(cluster_handle, cmd=['status'])
            # old mon returns status to outs...ick
            if ret == 0:
                outbuf += outs
        if ret:
            print >> sys.stderr, "status query failed: ", outs
            return ret
        print outbuf

        # this instance keeps the watch connection alive, but is
        # otherwise unused
        logwatch = rados.MonitorLog(cluster_handle, level, watch_cb, 0)

        # loop forever letting watch_cb print lines
        try:
            signal.pause()
        except KeyboardInterrupt:
            # or until ^C, at least
            return 0

    # read input file, if any
    inbuf = ''
    if parsed_args.input_file:
        try:
            with open(parsed_args.input_file, 'r') as f:
                inbuf = f.read()
        except Exception as e:
            print >> sys.stderr, 'Can\'t open input file {0}: {1}'.format(parsed_args.input_file, e)
            return 1

    # prepare output file, if any
    if parsed_args.output_file:
        try:
            outf = open(parsed_args.output_file, 'w')
        except:
            print >> sys.stderr, \
                'Can\'t open output file {0}: {1}'.\
                format(parsed_args.output_file, e)
            return 1

    # -s behaves like a command (ceph status).
    if parsed_args.status:
        childargs.insert(0, 'status')

    target = find_cmd_target(childargs)

    # Repulsive hack to handle tell: lop off 'tell' and target
    # and validate the rest of the command.  'target' is already
    # determined in our callers, so it's ok to remove it here.
    if len(childargs) and childargs[0] == 'tell':
        childargs = childargs[2:]

    # fetch JSON sigs from command
    # each line contains one command signature (a placeholder name
    # of the form 'cmdNNN' followed by an array of argument descriptors)
    # as part of the validated argument JSON object

    targets = [target]

    if target[1] == '*':
        if target[0] == 'osd':
            targets = [(target[0], o) for o in osdids()]
        elif target[0] == 'mon':
            targets = [(target[0], m) for m in monids()]

    final_ret = 0
    for target in targets:
        # prettify?  prefix output with target, if there was a wildcard used
        prefix = ''
        suffix = ''
        if not parsed_args.output_file and len(targets) > 1:
            prefix='{0}.{1}: '.format(*target)
            suffix='\n'

        ret, outbuf, outs = json_command(cluster_handle, target=target,
                                         prefix='get_command_descriptions')
        compat = False
        if ret == -errno.EINVAL:
            # send command to old monitor or OSD
            if verbose:
                print prefix + '{0} to old {1}'.format(' '.join(childargs), target[0])
            compat = True
            if parsed_args.output_format:
                childargs.extend(['--format', parsed_args.output_format])
            if parsed_args.threshold:
                childargs.extend(['--threshold', parsed_args.threshold])
            ret, outbuf, outs = send_command(cluster_handle, target, childargs,
                                             inbuf)
        elif ret:
            if ret < 0:
                outs = 'problem getting command descriptions from {0}.{1}'.format(*target)
        else:
            sigdict = parse_json_funcsigs(outbuf)

            if parsed_args.completion:
                return complete(sigdict, childargs, target)

            ret, outbuf, outs = new_style_command(parsed_args, childargs, target,
                                                  sigdict, inbuf, verbose)

        if ret < 0:
            ret = -ret
            print >> sys.stderr, prefix + 'Error {0}: {1}'.format(errno.errorcode[ret], outs)
            if len(targets) > 1:
                final_ret = ret
            else:
                return ret

        # this assumes outs never has useful command output, only status
        if outs:
            if compat and ret == 0:
                # old cli/mon would send status string to stdout on non-error
                print outs
            else:
                print >> sys.stderr, prefix + outs

        if (parsed_args.output_file):
            outf.write(outbuf)
        else:
            # hack: old code printed status line before many json outputs
            # (osd dump, etc.) that consumers know to ignore.  Add blank line
            # to satisfy consumers that skip the first line, but not annoy
            # consumers that don't.
            if parsed_args.output_format and \
               parsed_args.output_format.startswith('json') and \
               not compat:
                sys.stdout.write('\n');

            # if we are prettifying things, normalize newlines.  sigh.
            if suffix != '':
                outbuf = outbuf.rstrip()
            if outbuf != '':
                sys.stdout.write(prefix + outbuf + suffix)

        sys.stdout.flush()

    if (parsed_args.output_file):
        outf.close()

    if final_ret:
        return final_ret

    return 0

if __name__ == '__main__':
    sys.exit(main())
