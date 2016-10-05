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
    parser.add_option("-b", "--group", dest="group",
                  help="application name")
    parser.add_option("-c", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-d", "--env", dest="env",
              help="environment name")
    parser.add_option("-e", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-f", "--op0", dest="inputFile",
              help="environment name")
    parser.add_option("-g", "--op1", dest="common_properties",
                      help="increment field")
    parser.add_option("-j", "--op2", dest="table_properties_path",
                      help="increment field min bound")
    parser.add_option("-i", "--op3", dest="table_create_path",
                      help="increment field min bound")

    (options, args) = parser.parse_args()

    print "run_hive_create.py             ->     ****** common_properties used ==> " + str(options)
    
    
    if ( options.app == "" ):
        print "run_hive_create.py             -> ----App name not specified. Exiting----"
        sys.exit(1)
    
    
    
    
    if ( options.inputFile == "" ):
        print "run_hive_create.py             -> ----Input file with list of table names not specified. Exiting----"
        sys.exit(1)
    
    print "run_hive_create.py             -> inputFile = " + options.inputFile
    if not os.path.isfile(options.inputFile):
       print "run_gen.py           -> ERROR: Tables List file "+options.inputFile+" does not exists ***"
       sys.exit(1)

    return options
def main():
    
    options = arg_handle()
    #BEELINE_URL='jdbc:hive2://lbdp164a.uat.pncint.net:10000/default;principal=hive/lbdp164a.uat.pncint.net@PNCBANK.COM'
    envvars.populate(options.env,options.env_ver,options.app,options.sub_app) 
    BEELINE_URL="beeline -u '"+envvars.list['hive2JDBC']+"principal="+envvars.list['hive2Principal']+"'"


    print "run_hive_create.py             -> Beeline connect command - "+ BEELINE_URL
    #Read the list of table names from the generated file. 
    try:
        with open(options.inputFile) as fin:
            for line in fin:
                print "run_hive_create.py             -> ***************table name = " +line.strip() +"*********************"
                sys.stdout.flush()
                tableName=line.strip()
                envvars.clearList()
                envvars.populate(options.env,options.env_ver,options.app,options.sub_app)
                envvars.load_file(options.table_properties_path+"/"+tableName+".properties") 
                #stageTargetDir=envvars.list['stg_target_dir']
                #targetDir=envvars.list['target_dir']
                
                
                print "run_hive_create.py             ->       creating stage table - " +envvars.list['hv_db_stage']+"."+envvars.list['stage_table'] 
                print "run_hive_create.py             ->       stage target_dir = " + envvars.list['stg_target_dir']
                #comment/uncomment the below print statement to know/hide the exact command issued for creating hive table
                beeline_cmd = " ".join([BEELINE_URL,
                                        "-hiveconf hv_db_stage="+envvars.list['hv_db_stage'],
                                        " -hiveconf hv_db="+envvars.list['hv_db'],
                                        " -hiveconf stage_table="+tableName.strip(),
                                        " -hiveconf stg_target_dir="+envvars.list['stg_target_dir'],
                                        "-f",
                                        options.table_create_path+"/"+tableName+"_create_stg.hql"])
                print "run_hive_create.py             ->       " + beeline_cmd 
                
                #beeline -u ${BEELINE_URL} -hiveconf hv_db_stage="${hv_db_stage}" -hiveconf hv_db="${hv_db}" -hiveconf stage_table="$tableName" -hiveconf stg_target_dir="${stageTargetDir}" --silent=true -f "${table_create_path}"/${tableName}_create_stg.hql
                #hive -hiveconf hv_db_stage="${hv_db_stage}" -hiveconf hv_db="${hv_db}" -hiveconf stage_table="$tableName" -hiveconf stg_target_dir="${stageTargetDir}" -f "${table_create_path}"/${tableName}_create_stg.hql
                sys.stdout.flush()
                rc = os.system(beeline_cmd)
                sys.stdout.flush()
                #1>/tmp/${USER}.log
                if ( rc != 0 ):
                    print "run_hive_create.py             -> Create stage table script failed. please validate, fix and continue"     
                    sys.exit(1)
                   #hdfs dfs -chmod -R 777 ${stageTargetDir} 2>&1
                
                print "run_hive_create.py             ->       creating final table - " + envvars.list['hv_db']+"."+envvars.list['hv_table'] 
                print "run_hive_create.py             ->       final target_dir = " + envvars.list['target_dir']
                #Comment/Un-Comment the below print statement to know/hide the exact hive command used
                
                beeline_cmd = " ".join([BEELINE_URL,
                                        "-hiveconf hv_db_stage="+envvars.list['hv_db_stage'],
                                        " -hiveconf hv_db="+envvars.list['hv_db'],
                                        " -hiveconf table="+tableName.strip(),
                                        " -hiveconf target_dir="+envvars.list['target_dir'],
                                        "-f",
                                        options.table_create_path+"/"+tableName+"_create_parquet.hql"])
                
                print "run_hive_create.py             ->      " + beeline_cmd
                
                #beeline -u ${BEELINE_URL} -hiveconf hv_db_stage="${hv_db_stage}" -hiveconf hv_db="${hv_db}" -hiveconf table="$table" -hiveconf target_dir="${targetDir}" --silent=true -f "${table_create_path}"/${tableName}_create_parquet.hql 
                #hive -hiveconf hv_db_stage="${hv_db_stage}" -hiveconf hv_db="${hv_db}" -hiveconf table="$table" -hiveconf target_dir="${targetDir}" -f "${table_create_path}"/${tableName}_create_parquet.hql 
                
                #1>/tmp/${USER}.log
                sys.stdout.flush()
                rc = os.system(beeline_cmd)
                sys.stdout.flush() 
                if ( rc != 0 ):
                    print "run_hive_create.py             -> Create table script failed. please validate, fix and continue"     
                    sys.exit(1)
                print "run_hive_create.py             ->       Invalidate metadat - " +envvars.list['hv_db']+"."+envvars.list['hv_table'] 
                
                
                impalaCmd="impala-shell  -V -i "+envvars.list['impalaNode']+" -k --ssl -s xabdpimp --quiet -B -q  ' invalidate metadata "+envvars.list['hv_db']+"."+envvars.list['hv_table']+";invalidate metadata "+envvars.list['hv_db_stage']+"."+envvars.list['stage_table']+"; '"
                print "run_hive_create.py             ->      " + impalaCmd
                sys.stdout.flush()
                rc = os.system(impalaCmd)
                sys.stdout.flush()
                if (rc!= 0 ):
                    print "run_hive_create.py             -> Invalidate metadata failed"     
                    sys.exit(1)
       
    except IOError as e:
        if  e.errno != errno.ENOENT:
            raise IOError("exception file reading error")
        else:
            print("No Tablelist file found")


if __name__ == "__main__":
    main()
