#!.virtualenv/bin/python
'''
    stq - Simple Task Queue
    -----------------------
    (C) 2013 Daniel Fairhead <daniel.fairhead@om.org>
        an OMNIvision Project

    sqlite based very simple task queue system.

    ------------------------
    Usage:

    >>> from stq import TaskQueue
    >>> with stq.TaskQueue(config_filename) as tq:
    >>>     tq.save({'name':'do things', 'other':'task details', 'etc': data})

    and then later:

    >>> with stq.TaskQueue(config_filename) as tq:
    >>>
    >>>

'''

import sys
reload(sys)
sys.setdefaultencoding('utf-8') # pylint: disable=no-member


from os import makedirs
from os.path import isdir, join as pathjoin, abspath
from uuid import uuid1

from ConfigParser import SafeConfigParser

from flufl.lock import Lock

import json
from collections import defaultdict
from sqlite3 import OperationalError

from dictlitestore import DictLiteStore

valid_states = ('new', 'ready', 'running', 'done', 'failed', 'tmp', None)

##########################################################
# Errors:

ERR_USER_CANCELLED = -1
ERR_UNDEFINED_COMMAND = -2
ERR_COULD_NOT_RUN = -3
ERR_SOMETHING_UNKNOWN = -4

class InvalidConfigFile(Exception):
    ''' Invalid config file. '''
    pass


class InvalidSpecifiedDir(Exception):
    ''' The config file specifies a dir which I can't use! '''
    pass


class NoAvailableTasks(Exception):
    ''' There aren't any tasks available for you to do! '''
    pass

class TooBusy(Exception):
    ''' Currently there are already enough tasks running in that group '''
    pass

#########################################################
# Config object:

class Config(object):
    '''
    Slightly more friendly & error-checking ConfigParser wrapper, ONLY for
    STQ.  Implements a whole bunch of config file error checking on load.
    '''
    def __init__(self, filename):
        self.filename = filename

        try:
            self.config = SafeConfigParser()
            self.config.read(filename)
        except:
            raise InvalidConfigFile('Error loading config file:' + filename)

        self._require_dir('db')
        self._require_dir('tmp')
        self._require_dir('log')

        if not self.config.has_section('task_defaults'):
            self.config.add_section('task_defaults')

        if not self.config.has_option('task_defaults', 'stdout'):
            self.config.set('task_defaults', 'stdout',
                            pathjoin(self.get('DIRS', 'log'), 'tasks.log'))

        if not self.config.has_option('task_defaults', 'stderr'):
            self.config.set('task_defaults', 'stderr',
                            pathjoin(self.get('DIRS', 'log'), 'tasks.log'))


    def _require_section(self, secname):
        ''' raises an InvalidConfigFile error if the section doesn't exist '''
        if not self.config.has_section(secname):
            raise InvalidConfigFile(
                'Config file ({0}) does NOT contain required section:{1}'
                .format(self.filename, secname))

    def _require_section_option(self, secname, option):
        ''' raise an InvalidConfigFile if the section & option don't exist '''
        self._require_section(secname)

        if not self.config.has_option(secname, option):
            raise InvalidConfigFile(
                'Config file ({0}) does NOT contain required option:{1}->{2}'
                .format(self.filename, secname, option))

    def _require_dir(self, name):
        ''' checks if a dir exists, and if it doesn't, and cannot be created,
            then it will throw an InvalidSpecifiedDir error '''
        self._require_section_option('DIRS', name)
        dirname = self.get('DIRS', name)

        if not isdir(dirname):
            try:
                makedirs(dirname)
            except:
                raise InvalidSpecifiedDir(
                    "{0} doesn't exist, and I'm not allowed to make it."
                    .format(dirname))
        return True


    def get(self, section, option, default=None):
        ''' Either return the option, or the default. '''
        if self.config.has_option(section, option):
            return self.config.get(section, option)
        else:
            return default

    def groups(self):
        ''' return a list of available groups '''
        return [group for group in self.config.sections() if group != 'DIRS']


################################################################
# Task Queue:

class TaskQueue(object):
    ''' The actual Task Queue object. See Module docs '''

    def __init__(self, config_file):
        ''' initialise the task queue, from the config file '''
        self.config = Config(config_file)
        self.lock = Lock(pathjoin(self.config.get('DIRS', 'db'),
                         'TaskQueue.lock'))
        self.db = DictLiteStore(pathjoin(self.config.get('DIRS', 'db'),
                                'TaskQueue.db'), 'Tasks')

    def __enter__(self):
        ''' start of with TaskQueue(...) as t: block '''
        self.lock.lock()
        self.db.open()
        return self

    def __exit__(self, exptype, value, tb):
        ''' end of with ... block '''
        self.db.close()
        self.lock.unlock()

    def tasks(self, group=None, state=None):

        q = []
        if group:
            q.append(('group', '==', group))
        if state:
            q.append(('state', '==', state))

        return self.db.get(*q) # pylint: disable=W0142


    def active_groups(self):
        ''' return a list of all groups currently in the task list, and how
            many tasks they each are running '''

        # grouplist looks like:
        #
        # dict[groupname] -> dict[state] -> count
        # so you can do awesome things.

        grouplist = defaultdict(lambda:defaultdict(lambda:0))

        sql = u'SELECT Tasks."group", Tasks."state" From Tasks'

        try:
            rows = self.db.cur.execute(sql).fetchall()
        except OperationalError as err:
            # usually no such column, which means usually no rows.
            rowcount = self.db.cur.execute(u'SELECT Count(id) FROM Tasks')
            if rowcount.fetchone()[0] == 0:
                return grouplist
            else:
                raise err

        for rawgroups, rawstate in rows:

            groups = json.loads(rawgroups)
            state = json.loads(rawstate)

            if isinstance(groups, list):
                for g in groups:
                    grouplist[g][state] += 1
            else:
                grouplist[groups][state] += 1

        return grouplist

        ######################################
        # If for some reason it would be better to return dicts
        # rather than defaultdicts, then this is the code:
        #
        #to_return = {}
        #for groupname in grouplist:
        #    to_return[groupname] = dict(grouplist[groupname])

        #return to_return

    def grouplimit(self, groupname):
        ''' how many tasks can be run at the same time in this group? '''

        return int(self.config.get(groupname, 'limit', 1))


    def _getnexttask(self, group, new_state='running'):
        ''' get the next 'ready' task of this group. This should ONLY be called
        by self.getnexttask, not by end users. getnexttask checks that limits
        haven't been reached, etc. '''
        try:
            task = self.tasks(group, 'ready')[0]
            if new_state:
                task['state'] = new_state
                self.db.update(task, False, ('uid', '==', task['uid']))

            # Now we are going to start the task, import the defaults from
            # the group config:
            if self.config.config.has_section(group):
                for k, v in self.config.config.items(group):
                    if not k in task:
                        task[k] = v

            # and finally load defaults:
            if self.config.config.has_section('task_defaults'):
                for k, v in self.config.config.items('task_defaults'):
                    if not k in task:
                        task[k] = v

            return task
        except IndexError:
            raise NoAvailableTasks()


    def getnexttask(self, group=None, new_state='running'):
        ''' Get one available next task, as long as 'group' isn't overloaded.
            When the task is 'got', sets the state to new_state in the database.
            So this can be used as an atomic action on tasks. '''

        if group:
            running_tasks = self.active_groups()[group]['running']
            group_limit = self.grouplimit(group)

            if running_tasks < group_limit:
                return self._getnexttask(group, new_state)
            else:
                raise TooBusy()

        else: #no group specified.

            all_groups = self.active_groups()

            for groupname, grouptasks in all_groups.items():

                # already at limit:
                if grouptasks['running'] >= self.grouplimit(groupname):
                    continue

                # no ready tasks:
                if grouptasks['ready'] == 0:
                    continue

                # we have a winner! (a group with available tasks)
                return self._getnexttask(groupname, new_state)

            # if there are no ready tasks at all, then raise that exception

            if all((g['ready'] == 0 for g in all_groups.values())):
                raise NoAvailableTasks()

            # otherwise, there are availible tasks, but we're too busy.

            raise TooBusy()


    def save(self, data):
        if 'state' not in data:
            data['state'] = 'ready'

        if not 'uid' in data:
            data['uid'] = uuid1().hex

        if not 'group' in data:
            data['group'] = 'none'

        # If output files are not absolute paths, then place them in the config
        # file specified logfile directory.

        if 'stdout' in data and not data['stdout'] == abspath(data['stdout']):
            data['stdout'] = abspath(pathjoin(self.config.get('DIRS', 'log'),
                                              data['stdout']))

        if 'stderr' in data and not data['stderr'] == abspath(data['stderr']):
            data['stderr'] = abspath(pathjoin(self.config.get('DIRS', 'log'),
                                              data['stderr']))

        # And save it to the database.

        self.db.update(data, True, ('uid', '==', data['uid']))

        return data

    def get(self, uid):
        return self.db.get(('uid', '==', uid))

################################################################################
# Basic Commandline interface:


def simple_cli(database, todo, all_args):
    ''' a simple example CLI '''

    with TaskQueue(database) as tq:
        if todo == 'list':
            state = None if len(all_args) == 3 else all_args[3]

            tasks = tq.tasks(None, state)
            print '{0} {1} tasks:'.format(len(tasks), state if state else '')
            print '\n'.join([str(t) for t in tasks])

        elif todo == 'create':
            try:
                tname = all_args[3]
                tcommand = all_args[4]
                tgroup = all_args[5]
                if len(all_args) > 6:
                    args = all_args[6:]
                else:
                    args = None
            except IndexError:
                print 'Usage:'
                print all_args[0], 'create task_name command group'
                exit(1)

            tq.save( {'name': tname ,
                      'command': tcommand ,
                      'group': tgroup,
                      'command_args': args})

        elif todo == 'get':
            try:
                tgroup = all_args[3]
            except IndexError:
                tgroup = None

            try:
                task = tq.getnexttask(tgroup)
                print task
            except TooBusy:
                print "Currently doing enough jobs! I won't start another one!"
            except NoAvailableTasks:
                print 'There are no free tasks to do! Sorry!'

        elif todo == 'reset':
            for task in tq.tasks():
                task['state'] = 'ready'
                task['errcode'] = None
                task['pid'] = None
                tq.save(task)

if __name__ == '__main__':
    from sys import argv

    try:
        simple_cli(argv[1].strip(), argv[2].strip(), argv)
    except IndexError:
        print 'Usage:'
        print argv[0], 'config.ini list/create/get'
        exit(1)

