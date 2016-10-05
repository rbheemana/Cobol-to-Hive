#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, commands, envvars
import shutil 
from optparse import OptionParser

def get_keystore_root(key_store,app, common_root, service_root):
    keystore_root=""
    print "key_store = ", key_store
    if key_store.lower() == "common":
         keystore_root = common_root
    if key_store.lower() == "service":
         keystore_root = service_root + "/" + app
    if key_store.lower() == "user":
         keystore_root = "/user/" + os.environ['USER']
    
    if key_store is None or key_store.strip() == "":
         keystore_root = "/user/" + os.environ['USER']
         return keystore_root
    
    return keystore_root

def get_alias_name(key_store,jdbc_ref,user_id):
    password_alias=""
    user_id = user_id.lower()
    if key_store.lower() == "common":
         password_alias = jdbc_ref + "_" + user_id

    if key_store.lower() == "user":
        password_alias = jdbc_ref + "_" + user_id

    if key_store.lower() == "service":
         password_alias = user_id
    return password_alias

def main():
    env, env_ver,app, user_id, pwd, key_store,jdbc_ref= arg_handle()
    envvars.populate(env,env_ver,"bdh","data_ingest")
    #keystore_root = get_keystore_root(key_store,app, \
    #                   envvars.list['hdfs_common_keystore_root'], \
    #                   envvars.list['hdfs_service_keystore_root'])
    keystore_root = envvars.list['hdfs_' + key_store + '_keystore_root'] 
    print "keystore root = ", keystore_root
    alias_name= get_alias_name(key_store,jdbc_ref,user_id)
    key_provider = "jceks://hdfs/" + keystore_root  + "/" +   user_id.upper() + ".jceks"
    
    #command to delete alias 
    hadoop_cmd = "hadoop credential delete " + alias_name + " -f -provider " + key_provider
    print "delete mannually (if needed) alias using command ==> "
    print "     " + hadoop_cmd 
    rc,status = commands.getstatusoutput(hadoop_cmd)
    print(status)

    #command to create alias 
    hadoop_cmd = "hadoop credential create " + alias_name + " -provider " + key_provider
    print "Generating alias using command ==> "
    print "     " + hadoop_cmd + " -v " + pwd.replace("\$","\\\$")
    hadoop_cmd = hadoop_cmd +  " -v " + pwd.replace("$","\$")
    rc,status = commands.getstatusoutput(hadoop_cmd)
    print(status)


def arg_handle():
    usage = "\n   create_pwd_alias.py --env --env_ver --jdbc_ref --user_id --password [ --key_store ] "
    parser = OptionParser(usage)
    parser.add_option("-e", "--env", dest="env",
              help="env d/t/p to represent environment.")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="env_ver 01/02 to represent environment.")
    parser.add_option("-a", "--app", dest="app",
              help="app folder under  which key store should be stored.")
    parser.add_option("-u", "--user_id", dest="user_id",
              help="user id for which alias needs to be created")
    parser.add_option("-p", "--password", dest="password",
              help="password for which alias needs to be created")
    parser.add_option("-j", "--jdbc_ref", dest="jdbc_ref",
              help="jdbc_ref for which password aliash should be created.")
    parser.add_option("-k", "--key_store", dest="key_store",
              help="password for which alias needs to be created")

    (options, args) = parser.parse_args()


    print("create_pwd_alias.py           -> Input      : " + str(options)) 
    if  options.env is None or options.env == "":
        parser.error("Argument, env, is required.")
    if  options.env_ver is None or options.env_ver == "":
        parser.error("Argument, env_ver, is required.")
    if  options.user_id is None or options.user_id== "":
        parser.error("Argument, user_id, is required.")
    if  options.jdbc_ref is None or options.jdbc_ref == "":
        parser.error("Argument, jdbc_ref, is required.")
    password = options.password
    if  password is None or options.password== "":
          password=raw_input("Enter password for '"+ options.user_id +"' : " )
          if password.strip() == "":
              parser.error("Argument, password, is required.")
    key_store=options.key_store
    if  key_store is None or key_store == "":
          key_store="user"
          print "Using default value '" + key_store +  "'. Other usable values are 'service' or 'common' "
    
    return options.env, options.env_ver, options.app, options.user_id, password, key_store, options.jdbc_ref


if __name__ == "__main__":
    main()
