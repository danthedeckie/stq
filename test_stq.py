#!.virtualenv/bin/python

from os.path import exists
from os import remove
from shutil import rmtree

import unittest
import stq

class BaseCase(unittest.TestCase):
    ''' a generic unittest class for you to base everything off. '''
    pass



################################################################################
#
# Config
#
################################################################################

CONFIG_FILE = '__test.conf'
CONFIG_DEFAULTS = \
'''
[DIRS]
db=__test
tmp=__test
log=__test

'''

def make_config():
    with open(CONFIG_FILE,'w') as tfile:
        tfile.write(CONFIG_DEFAULTS)

def remove_config():
    if exists(CONFIG_FILE):
        remove(CONFIG_FILE)
    if exists('__test'):
        rmtree('__test')



class BaseCaseClass_Config(BaseCase):
    ''' Module docstring:

    Slightly more friendly & error-checking ConfigParser wrapper, ONLY for
    STQ.  Implements a whole bunch of config file error checking on load.

    ----------
    Methods:
    _require_section, _require_section_option, _require_dir, get,
    ----------
    '''
    def setUp(self):
        make_config()
        self.config = stq.Config(CONFIG_FILE)

    def tearDown(self):
        remove_config()


class Test_Config__require_section(BaseCaseClass_Config):
    ''' Method docstring:
     raises an InvalidConfigFile error if the section doesn't exist
    ----------
    Args: ['secname']
    '''
    def test_empty(self):
        with self.assertRaises(TypeError):
            self.config._require_section()

    def test_non_existant(self):
        with self.assertRaises(stq.InvalidConfigFile):
            self.config._require_section('none')

    def test_existant(self):
        self.assertEqual(self.config._require_section('DIRS'), None)


class Test_Config__require_section_option(BaseCaseClass_Config):
    ''' Method docstring:
     raise an InvalidConfigFile if the section & option don't exist
    ----------
    Args: ['secname', 'option']
    '''
    def test_empty_args(self):

        with self.assertRaises(TypeError):
            self.config._require_section_option()


    def test_all_zeros(self):
        with self.assertRaises(stq.InvalidConfigFile):
            self.config._require_section_option(0, 0)


class Test_Config__require_dir(BaseCaseClass_Config):
    ''' Method docstring:
     checks if a dir exists, and if it doesn't, and cannot be created,
            then it will throw an InvalidSpecifiedDir error
    ----------
    Args: ['name']
    '''
    def test_empty_args(self):

        with self.assertRaises(TypeError):
            self.config._require_dir()


    def test_non_existant(self):
        with self.assertRaises(stq.InvalidConfigFile):
            self.config._require_dir('none')


class Test_Config_get(BaseCaseClass_Config):
    ''' Method docstring:
     Either return the option, or the default.
    ----------
    Args: ['section', 'option', 'default']
    '''
    def test_empty_args(self):

        with self.assertRaises(TypeError):
            self.config.get()


    def test_non_existant(self):
        self.assertTrue(self.config.get('none','none', True))



################################################################################
#
# TaskQueue
#
################################################################################

class BaseCaseClass_TaskQueue(BaseCaseClass_Config):
    ''' Module docstring:
     The actual Task Queue object. See Module docs
    ----------
    Methods:
    __enter__, __exit__, tasks, getnexttask, save, get,
    ----------
    '''
    def setUp(self):
        make_config()
        self.taskqueue = stq.TaskQueue(CONFIG_FILE)
        # because taskqueue is supposed to be used with 'with' normally:
        self.taskqueue.__enter__()

    def tearDown(self):
        self.taskqueue.__exit__(0, 0, 0)
        remove_config()


class Test_TaskQueue_tasks(BaseCaseClass_TaskQueue):
    ''' Method docstring:
    None
    ----------
    Args: ['group', 'state']
    '''
    def test_empty_args(self):

        self.taskqueue.tasks()


    def test_all_zeros(self):
        self.taskqueue.tasks(0, 0)

class Test_TaskQueue_active_groups(BaseCaseClass_TaskQueue):
    ''' Method docstring:
    return a list of all groups currenly in the task list, and
    how many tasks they each are running
    ----------
    Args: None
    '''
    def test_empty_args(self):
        ''' if the db is empty, it should return an empty dict '''

        self.assertEqual(self.taskqueue.active_groups(), {})

    def test_with_single_task_with_one_group(self):
        ''' only one task in the queue, and so it's ready to go! '''

        self.taskqueue.save({u'name': u'stuff', u'group': u'group1'})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'group1': {u'ready': 1}})

    def test_with_single_task_with_no_group(self):
        ''' only one task in the queue, with no group,
            so auto-assigned group none'''

        self.taskqueue.save({u'name': u'stuff'})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'none': {u'ready': 1}})


    def test_multiple_tasks_with_one_group(self):
        ''' multiple tasks in one group, should be fine. '''

        self.taskqueue.save({u'name': u'stuff', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff1', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff2', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff3', u'group': u'group1'})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'group1': {u'ready': 4}})


    def test_multiple_tasks_with_one_group_different_states(self):
        ''' multiple tasks in one group, but with different states '''

        self.taskqueue.save({u'name': u'stuff', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff1', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff2', u'group': u'group1',
                             u'state': u'failed'})
        self.taskqueue.save({u'name': u'stuff3', u'group': u'group1'})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'group1': {u'ready': 3, u'failed': 1}})

    def test_multiple_tasks_with_different_single_groups(self):
        ''' multiple tasks in one group, should be fine. '''

        self.taskqueue.save({u'name': u'stuff', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff1', u'group': u'group2'})
        self.taskqueue.save({u'name': u'stuff2', u'group': u'group1'})
        self.taskqueue.save({u'name': u'stuff3', u'group': u'group2'})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'group1': {u'ready': 2},
                          u'group2': {u'ready': 2}})


    def test_with_single_task_with_multiple_groups(self):
        ''' only one task in the queue, but with multiple groups '''

        self.taskqueue.save({u'name': u'stuff',
                             u'group': [u'group1', u'group2']})

        self.assertEqual(self.taskqueue.active_groups(),
                         {u'group1': {u'ready': 1},
                          u'group2': {u'ready': 1}})







class Test_TaskQueue_getnexttask(BaseCaseClass_TaskQueue):
    ''' Method docstring:
    None
    ----------
    Args: ['group', 'new_state']
    '''
    def test_empty_args(self):

        with self.assertRaises(stq.NoAvailableTasks):
            self.taskqueue.getnexttask()


    def test_all_zeros(self):
        with self.assertRaises(stq.NoAvailableTasks):
            self.taskqueue.getnexttask(0, 0)


class Test_TaskQueue_save(BaseCaseClass_TaskQueue):
    ''' Method docstring:
    None
    ----------
    Args: ['data']
    '''
    def test_empty_args(self):

        with self.assertRaises(TypeError):
            self.taskqueue.save()


    def test_all_zeros(self):
        with self.assertRaises(TypeError):
            self.taskqueue.save(0)


class Test_TaskQueue_get(BaseCaseClass_TaskQueue):
    ''' Method docstring:
    None
    ----------
    Args: ['uid']
    '''
    def test_empty_args(self):

        with self.assertRaises(TypeError):
            self.taskqueue.get()


    def test_all_zeros(self):
        self.taskqueue.get(0)


if __name__ == '__main__':
    unittest.main()
