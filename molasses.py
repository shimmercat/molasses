# Copyright (c) 2015, Zunzun AB
# All rights reserved.
# 
# Redistribution and use in source and binary forms, 
# with or without modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of 
#    conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list
#    of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to
#    endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT 
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF 
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
A script to execute groups of processess in a particular cgroup and with CPU time highly limitted.
This script only uses Python standard library.

Usage (command itself will vary depending on if you install it with PIP or use the script directly) :

To create a new browser instance at 10% of maximum processor speed:

   $ molasses launch --speed=10pct -- google-chrome


"""


import os.path
import os
import sys
import shutil
import random
import pwd
import grp
import shlex
import sqlite3
import subprocess
import re
import argparse
import signal

MOLASSES_SQ_ = "molasses.sq3"


def speed_str_to_fraction(speed_str):
    mo = re.match(r'(([0-9]+(\.[0-9]*)?)|([0-9]*\.[0-9]+))(pct|%)', speed_str)
    if mo:
        num = mo.group(1)
        return float(num) / 100.0
    mo = re.match(r'(([0-9]+(\.[0-9]*))|([0-9]*\.[0-9]+))', speed_str)
    if mo:
        num = mo.group(1)
        return float(num)
    raise ValueError("Could not parse speed")


def split_on_double_dash(argv):
    try:
        pos = argv.index('--')
    except ValueError:
        return (argv, [])
    else:
        if pos == len(argv) - 1 :
            return (argv[:pos], [])
        else:
            return (argv[:pos], argv[pos+1:])


def create_cg_name():
    rng = random.Random()
    rnd_part = ''.join( rng.choice('abcdefghi') for i in range(5) )
    return 'molasses_cg_' + rnd_part


def create_cg(cg_name, subsystems):
    current_user_id = os.getuid()
    current_user_name = pwd.getpwuid(current_user_id)[0]
    current_group_id = os.getgid()
    current_user_group = grp.getgrgid(current_group_id)[0]
    cmd_line = ['sudo', 'cgcreate'] + \
        ['-a', str(current_user_name)+':'+str(current_user_group)] + \
        ['-g', cg_handle(cg_name, subsystems)] + \
        ['-t', str(current_user_name)+':'+str(current_user_group)]
    print('Executing: ', ' '.join(cmd_line))
    subprocess.check_call(cmd_line)


def delete_cg(cg_name, subsystems):
    cmd_line = ['sudo', 'cgdelete'] + \
        [cg_handle(cg_name, subsystems)]
    print('Executing: ', ' '.join(cmd_line))
    subprocess.check_call(cmd_line)


def cg_handle(cg_name, subsystems):
    return ','.join(subsystems) + ':/' + cg_name


bookkeeper_conn = None

def get_bk_conn():
    global bookkeeper_conn

    if bookkeeper_conn is None:
        bookkeeper_conn = sqlite3.Connection(MOLASSES_SQ_)
        create_bookeeper(bookkeeper_conn)
    return bookkeeper_conn


def create_bookeeper(conn):
    cursor = conn.cursor()

    cursor.executescript("""CREATE TABLE IF NOT EXISTS  created_cgs(
    cgname VARCHAR PRIMARY KEY
) ;
CREATE TABLE IF NOT EXISTS subsystems(
    ssname VARCHAR,
    ofcg VARCHAR,
    seqno  INTEGER,
    FOREIGN KEY (ofcg) REFERENCES created_cgs(cgname)
);
    """)


def bookkeep_created_cg(cg_name, subsystems):
    conn = get_bk_conn()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO created_cgs VALUES (?)", (cg_name,))
    cursor.executemany("INSERT INTO subsystems VALUES (?,?,?)", [
        (subsystem, cg_name, seqno) for (seqno,subsystem) in enumerate(subsystems)
    ])
    conn.commit()


def cmd_launch(args, after_split):
    new_cg = create_cg_name()
    subsystems = ['cpu']
    create_cg(new_cg, subsystems)
    bookkeep_created_cg(new_cg, subsystems)

    speed_fraction = speed_str_to_fraction(args.speed)
    # cmd_line = shlex.split("sudo cgset -r cpu.cfs_period_us={0} -r cpu.cfs_quota_us={1} {2}".format(
    #     100000,
    #     int(100000*speed_fraction),
    #     new_cg
    # ))
    cmd_line = shlex.split("sudo cgset -r cpu.cfs_period_us={0} {1}".format(
        100000,
        new_cg
    ))
    print('Executing: ', ' '.join(cmd_line))
    subprocess.check_call(cmd_line)
    cmd_line = shlex.split("sudo cgset -r cpu.cfs_quota_us={0} {1}".format(
        int(100000*speed_fraction),
        new_cg
    ))
    print('Executing: ', ' '.join(cmd_line))
    subprocess.check_call(cmd_line)

    cmd_line = shlex.split( "cgexec -g {0}".format(cg_handle(new_cg, subsystems)))
    cmd_line += after_split

    print("Executing: ", ' '.join(cmd_line))
    subprocess.check_call(cmd_line)


def kill_tasks(cgname):
    cg_tasks_file = '/sys/fs/cgroup/cpu/' + cgname + '/tasks'
    try:
        items = open(cg_tasks_file)
    except FileNotFoundError:
        return
    for line in items:
        task_id = int(line)
        try:
            os.kill(task_id, signal.SIGKILL)
        except ProcessLookupError:
            pass


def cmd_killall(args, after_split):
    conn = get_bk_conn()
    cg2subsystems = get_cg2subsystems(conn)# print(cg2subsystems)

    cursor = conn.cursor()
    ready_for_unlink = True
    for (cgname, subsystem_list) in cg2subsystems.items():
        kill_tasks(cgname)
        try:
            delete_cg(cgname, subsystem_list)
        except subprocess.CalledProcessError:
            ready_for_unlink = False
        else:
            cursor.execute("""DELETE FROM subsystems WHERE ofcg = ? ; """, (cgname,))
            cursor.execute("""DELETE FROM created_cgs WHERE cgname = ? ; """, (cgname,))
            conn.commit()

    if ready_for_unlink:
        conn.close()
        os.unlink(MOLASSES_SQ_)


def get_cg2subsystems(conn):
    cursor = conn.cursor()
    cursor.execute("""SELECT ofcg, ssname FROM subsystems ORDER BY  ofcg, seqno""")
    cg2subsystems = {}
    for (cgname, subsystem) in cursor:
        cg2subsystems.setdefault(cgname, []).append(subsystem)

    return cg2subsystems


def main():
    aparser = argparse.ArgumentParser(
        description="Run programs and groups of programs slowly"
    )

    subparsers = aparser.add_subparsers()

    launch_slow = subparsers.add_parser('launch', help="Launch a slow program" )
    launch_slow.add_argument('--speed', '-s', type=str, default="10pct" ,
                             help="Speed of the program (use pct suffix for percent)")
    launch_slow.set_defaults(func=cmd_launch)

    kill_all = subparsers.add_parser('killall', help="Kill all processes and remove all cgroups created from here")
    kill_all.set_defaults(func=cmd_killall)

    till_split, after_split = split_on_double_dash(sys.argv)

    args = aparser.parse_args(till_split[1:])

    args.func(args, after_split)


if __name__ == '__main__':
    main()
