#!.virtualenv/bin/python
'''
    run_tasks.py
    ------------

    Run tasks in the background.

    This file, when called to run as a script, does the UNIX fork trick[1]
    and so daemonises, detaching from the parent process tree.

    It then examines the task queue, looking for any available tasks to run.
    If there are any, then it runs them, keeping an eye on them, and updating
    the task database to keep track of it. (if it dies what the status code is,
    times it starts and stops, etc, etc.

    --

    [1] - nothing like Uri Geller's spoon trick, in case you're wondering
'''

import sys
import subprocess
import signal
import json
import os
from os.path import abspath, isfile, join as pathjoin, dirname

from ConfigParser import ConfigParser

import stq

class TaskRunner(object):
    '''
    the main 'taskrunner' object.  This keeps track of the task ID, saving,
    loading, etc.
    '''

    task = None
    process = None

    def __init__(self, configfile):
        ''' check that the config file is valid, and load data from it '''

        self.configfile = configfile
        self.config = ConfigParser()
        try:
            self.config.read(configfile)
        except:
            raise stq.InvalidConfigFile()

    def save(self):
        ''' write any updated info in self.task to the task queue '''

        with self.TQ() as taskqueue:
            taskqueue.save(self.task)

    def TQ(self):
        ''' return the task queue object '''

        stqconfig = self.config.get('FILES', 'STQ_Config', self.configfile)

        if isfile(stqconfig):
            return stq.TaskQueue(stqconfig)
        else:
            return stq.TaskQueue(pathjoin(dirname(self.configfile), stqconfig))

    def fail(self, errcode):
        ''' something went wrong.  update the state, and save '''

        if self.task:
            self.task['state'] = 'failed'
            self.task['errcode'] = errcode
            self.save()

    def run(self):
        ''' actually run a task.  Note: This DOES NOT fork and daemonise!
            running this directly will run the task and block until done. '''

        cmd = self.get_command(self.task['command'])

        if not cmd:
            self.fail(stq.ERR_UNDEFINED_COMMAND)

            print 'Config file ({0}) missing:'.format( self.configfile)
            print '    [commands]'
            print '    {0}=/.../.../...'.format(self.task['command'])
            print '(the command I\'m trying to run!)'

            return False

        if not isfile(cmd):
            cmd = abspath(pathjoin(dirname(self.configfile), cmd))


        # prepare command to run:
        cmd_args = self.task.get('command_args', None)

        if isinstance(cmd_args, list):
            cmdlist = [cmd] + cmd_args
        elif cmd_args == None:
            cmdlist = [cmd]
        else:
            cmdlist = [cmd, cmd_args]

        if '__json__' in cmdlist:
            cmdlist[cmdlist.index('__json__')] = json.dumps(self.task)

        # update the PYTHONPATH enviroment env, so that any scripts called can
        # use our nice shiny virtualenv...

        os.environ['PYTHONPATH'] = ':'.join([x for x in sys.path if x])

        try:
            # Now open the handles needed, and start off the process.
            if self.task['stderr'] == self.task['stdout']:
                with open(self.task['stdout'],'a') as outfile:
                    print ('Running:', cmdlist,
                           ' output:', self.task['stdout'])
                    self.process = subprocess.Popen(cmdlist,
                                                    stdout=outfile,
                                                    stderr=subprocess.STDOUT)
            else:
                with open(self.task['stdout'],'a') as outfile:

                    with open(self.task['stderr'],'a') as errfile:
                        print ('Running:', cmdlist,
                               ' stdout:', self.task['stdout'],
                               ' stderr:', self.task['stderr'])

                        self.process = subprocess.Popen(cmdlist,
                                                        stdout=outfile,
                                                        stderr=errfile)

            # Update the db.
            self.task['state'] = 'running'
            self.task['pid'] = self.process.pid
            self.save()
        except OSError as err:
            self.fail(stq.ERR_COULD_NOT_RUN)

            print "Couldn't run the specified command!"
            print err
            print ' '.join(cmdlist)
            return False

        # OK. It seemed to start well enough.
        # Let's wait for it it finish, I guess.

        try:
            self.process.wait()

        except Exception as err: # pylint: disable=broad-except
            self.task['state'] = 'failed'
            self.task['errcode'] = stq.ERR_SOMETHING_UNKNOWN
            self.save()

            print 'Something went wrong!'
            print err
            return False

        if self.process.returncode != 0:
            self.fail(self.process.returncode)

            print 'It failed while running!'
            self.task['state'] = 'failed'
            self.task['errcode'] = self.process.returncode
            self.task['message'] = 'Failed while running!'
            self.save()
            print self.task
            return False

        # Apparently it finished alright!
        self.task['state'] = 'finished'
        self.save()
        return True

    def get_command(self, cmdname):
        '''
            check that cmdname is actually a valid command to run.
        '''

        try:
            return self.config.get('commands', cmdname)
        except:
            print ('Requested Task:"{0}" not in [commands]'
                   ' (in config file)!'.format(cmdname))
            return False



def main(configfile):
    '''
        This function should NOT be run from anything OTHER THAN this
        module, when it's used as a stand-alone script.  It forks and
        becomes a daemon, and then will start processing tasks, until
        there are none left to do.
    '''

    #####################################################################
    # Loop of getting tasks and running them:

    runner = TaskRunner(configfile)

    while True:
        try:
            try:
                with runner.TQ() as taskqueue:
                    runner.task = taskqueue.getnexttask()
            except stq.NoAvailableTasks:
                # There are no tasks to run! Woot!
                exit(0)
            except stq.TooBusy:
                print 'Sorry! Too Busy!'
                exit(1)

            if runner.task:

                def term_handler(num, stack): # pylint: disable=unused-argument
                    ''' This is called if the current process
                        (the controlling process) is sent a KILL or DIE signal.
                        Instead of just curling up in a heap and dieing, it
                        stops the child process, and then sets the task
                        queue status to killed by user, and then dies.'''

                    runner.process.terminate()
                    runner.fail(stq.ERR_USER_CANCELLED)
                    exit(1)

                # assign the signal handler:

                signal.signal(signal.SIGTERM, term_handler)

                # run the current task (this could take a very long time)

                runner.run()

                # and set the signal handler back to the default python one.

                signal.signal(signal.SIGTERM, signal.SIG_DFL)

            else:
                # Hm. No tasks, but no exceptions. odd.
                exit(1)

        except KeyboardInterrupt:
            if runner.task:
                runner.fail(stq.ERR_USER_CANCELLED)
            exit(1)

###############################################################################

if __name__ == '__main__':
    try:
        CONFIG = abspath(sys.argv[1])
    except:
        print 'Usage:'
        print '   run_next.py $configfile'
        print 'Where $configfile is the name of the config file (duh)'
        exit(1)

    try:
        if os.fork():
            exit(0)
    except OSError:
        exit(1)

    os.umask(0)
    os.chdir('/')
    os.setsid()

    try:
        if os.fork():
            exit(0)
    except OSError:
        exit(1)

    main(CONFIG)
