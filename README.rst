====
stq
====

A Simple Task Queue, using Sqlite3, for Python.

VERY VERY EARLY ALPHA!!!

======
Why?!?
======

I needed a simple task queue, and didn't want to install redis, or rabbitmq, or similar.  Surely just Sqlite3 and a python install should be enough, right?

===========
Basic Usage
===========

You need a configuration file `config.ini` ::

    [DIRS]
    db=/tmp
    tmp=/tmp
    log=/tmp

Something like this: ::

    from stq import TaskQueue

    with stq.TaskQueue('config.ini') as tq:
        tq.save({'name': 'update stuff',
                 'group': 'basic',
                 'other': 'details',
                 'as': 'required'})

Then later, to retrieve tasks: ::

    with stq.TaskQueue('config.ini') as tq:
        try:
            task = tq.getnexttask()

        except stq.TooBusy:
            print "I'm doing enough already! Leave me alone!"

        except stq.NoAvailableTasks:
            print 'Nothing to do...'


The idea is that you can define as many groups of task types as you want, say if you're automating various workstations backing up, you may only want each workstation to be able to do one task at a time, so you don't overload its network, but you'd be fine if at the same time the server wanted to update yum, or apt, say. But it shouldn't try to do multiple of those at the same time.
