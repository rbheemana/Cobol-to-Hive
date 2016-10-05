#!/usr/bin/python
import sys, os, commands
# Purpose: to store the environment variables


list = {}

def clearList():
	 list = {}

def populate(env, env_ver, app,sub_app):
   list['ENV'] = env
   list['ENV_VER'] = env_ver
   list['app'] = app
   list['sub_app'] = sub_app
   list['min_bound'] = ''
   list['max_bound'] = ''
   rc,env_user=commands.getstatusoutput("echo ${USER}")
   list['USER'] = env_user
   list['happ'] = app
   if len(app.split("/")) > 1:
   	  list['happ'] = app.split("/")[0]
   
   common_properties = '/data/bdp' + env + '/bdh/' + env_ver + '/global/config/oozie_global.properties'
   with open(common_properties) as fin:
        for line in fin:
            if  line.strip() != '':
                if  line.strip()[0] != '#':
                    varline = line.strip().replace('$', '').format(**list)
                    tokens = varline.split('=')
                    list[tokens[0]] = '='.join(tokens[1:])

def populateAppProperties(env, env_ver, appParam, fileRelativePath):
   list['ENV'] = env
   list['ENV_VER'] = env_ver
   list['app'] = appParam
   list['happ'] = appParam
   if len(appParam.split("/")) > 1:
   	  list['happ'] = appParam.split("/")[0]
   list['min_bound'] = ''
   list['max_bound'] = ''
   rc,env_user=commands.getstatusoutput("echo ${USER}")
   list['USER'] = env_user
   fileFullPath = '/var/bdh_code/bdp' + env + '/' + appParam + '/' + env_ver + '/' + fileRelativePath

   with open(fileFullPath) as fin:
        for line in fin:
            if  line.strip() != '':
                if  line.strip()[0] != '#':
                    varline = line.strip().replace('$', '').format(**list)
                    tokens = varline.split('=')
                    list[tokens[0]] = '='.join(tokens[1:])


def load_file(file_path):
   with open(file_path) as fin:
        for line in fin:
            if  line.strip() != '':
                if  line.strip()[0] != '#':
                    varline = line.strip().replace('$', '').format(**list)
                    tokens = varline.split('=')
                    list[tokens[0]] = '='.join(tokens[1:])

if __name__ == "__main__":
    main()
