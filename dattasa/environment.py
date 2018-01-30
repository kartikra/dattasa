import os, sys, subprocess, json
import pprint

def get_os_env(env_file=os.environ['HOME'] + "/.bashrc"):
    '''
    Return all the os variables in the environment profile file
    :param env_file: Specify the profile file from where to source. Default=$HOME/.bashrc
    :return:
    '''
    source = "source " + env_file
    dump = '/usr/bin/python -c "import os, json;print json.dumps(dict(os.environ))"'
    pipe = subprocess.Popen(['/bin/bash', '-c', '%s && %s' %(source,dump)], stdout=subprocess.PIPE)
    if sys.version_info.major == 3:
        env = json.loads(str(pipe.stdout.read(),'utf-8'))
    else:
        env = json.loads(pipe.stdout.read())

    return env


if __name__ == '__main__':
    my_env = get_os_env()
    pprint.pprint(my_env)
