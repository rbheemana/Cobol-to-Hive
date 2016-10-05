#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE

def arg_handle():
    usage = "usage: run_gen.py (options)"
    parser = OptionParser(usage)
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-g", "--group", dest="group",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-b", "--op0", dest="config_file",
              help="environment name")
    parser.add_option("-c", "--op1", dest="key_store",
                      help="increment field")
    parser.add_option("-d", "--op2", dest="step",
                      help="increment field min bound")

    (options, args) = parser.parse_args()

    if options.env == "": 
       print "run_gen.py           -> ERROR: environment value (p/q/d/t) is missing)"
       sys.exit(1)
    
    if options.env_ver == "":
       print "run_gen.py           -> ERROR: env version value (01/02 ) is missing)"
       sys.exit(1)
    if options.config_file == "":
       print "run_gen.py           -> ERROR: config file with table information is missing)"
       sys.exit(1)


    
    if options.key_store == "":
       options.key_store="service"
       print "run_gen.py           -> INFO: **** Using default key_store= service ******"
    
    if (options.key_store != "service" ) and ( options.key_store != "user" ) and ( options.key_store != "common" ):
        print "run_gen.py           -> ERROR: **** 4th parameter key_store should be either service/user/common ****"
        sys.exit(1)
    
    if ( options.step == "" ):
       options.step="all"


    print("run_gen.py           -> Input      : " + str(options)) 
    return options, sys.argv
    


def main():
    
    options, args = arg_handle()
    
    envvars.populate(options.env,options.env_ver,options.app,options.sub_app)
    config_file_path = envvars.list['lfs_app_config']+"/"+options.config_file
    if not os.path.isfile(config_file_path):
       print "run_gen.py           -> ERROR:   config file "+config_file_path+" does not exists ***"
       sys.exit(1)
    print "**************************************************************************************************"
    args = " ".join([envvars.list['lfs_global_scripts'] + "/getmetadata.py ",
                     "-e "+ options.env.strip(),
                     "-a "+options.app,
                     "-u "+options.sub_app,
                     " -v "+options.env_ver,
                     " -k "+options.key_store.strip(),
                     " -s "+options.config_file]) 
    getmetadata_script = "python " + args
    if ( options.step == "all" ) or ( options.step == "1" ):
       print("run_gen.py           -> STEP-1    : ************************************************************************")
       print("run_gen.py           -> Invoked   : " + getmetadata_script) 
       call = subprocess.Popen(getmetadata_script.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT) 
       while True:
          line = call.stdout.readline()
          if not line:
              break
          print line.strip()
          sys.stdout.flush()
       call.communicate()

       rc = call.returncode 
       if rc != 0:
           print "run_gen.py           -> getting  metadata using " + args + " is not successful."
           sys.exit(1)
       else:
           print "run_gen.py           -> getting  metadata command was successful."
    if ( options.step == '1' ):
       sys.exit(0)
    print "**************************************************************************************************"
    
    args = " ".join([envvars.list['lfs_global_scripts'] + "/generate.py -s",
                        options.config_file,
                        "-m",
                        envvars.list['lfs_app_config']+"/"+options.config_file+".meta",
                        "-w",
                        envvars.list['lfs_app_workflows']+"/wf_db_ingest",
                        "-k",
                        options.key_store,
                        "-e "+ options.env.strip(),
                        "-a "+options.app,
                        "-u "+options.sub_app,
                        "-v "+options.env_ver])
    generate_script = "python " + args 
    if ( options.step == "all" ) or ( options.step == "2" ):
        print("run_gen.py           -> STEP-2    : ************************************************************************")
        print("run_gen.py           -> Invoked   : " + generate_script) 
        call = subprocess.Popen(generate_script.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT) 
        while True:
           line = call.stdout.readline()
           if not line:
               break
           print line.strip()
           sys.stdout.flush()
        call.communicate()
        
        rc = call.returncode 
        if rc != 0:
            print "run_gen.py           -> Generating create scripts and properties file is not successful."   
            sys.exit(1)
        else:
            print "run_gen.py           -> generating create scripts and properties files was successful."
    if ( options.step == '2' ):
       sys.exit(0)
    print "**************************************************************************************************"
      
    
    if ( options.step == "all" ) or ( options.step == "3" ):
       
       print("run_gen.py           -> STEP-3    : ************************************************************************") 
       args = " ".join([envvars.list['lfs_global_scripts'] + "/run_hive_create.py",
                         "--app "+options.app,
                         "--subapp "+options.sub_app,
                         "--env " +options.env,
                         "--op0 "+envvars.list['lfs_app_config']+"/"+options.config_file+".list",
                         "--op1 "+envvars.list['lfs_global_config']+"/oozie_global.properties",
                         "--env_ver "+options.env_ver,
                         "--op2 "+envvars.list['lfs_app_workflows']+"/wf_db_ingest",
                         "--op3 "+envvars.list['lfs_app_src']+"/hive"])
                         
       hivecreate_script = "python " + args
       print("run_gen.py           -> Invoked   : " + hivecreate_script) 
       call = subprocess.Popen(hivecreate_script.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT) 
       while True:
          line = call.stdout.readline()
          if not line:
              break
          print line.strip()
          sys.stdout.flush()
       call.communicate()

       rc = call.returncode
       #os.system(hivecreate_script) 
       if rc != 0:
           print "run_gen.py           -> Creating hive tables is not successful."
           print rc
           sys.exit(1)
       else:
            print "run_gen.py           -> Completed executing create table scripts."  
    print "run_gen.py           -> Completed executing create table scripts."
    print "**************************************************************************************************"


if __name__ == "__main__":
    main()





