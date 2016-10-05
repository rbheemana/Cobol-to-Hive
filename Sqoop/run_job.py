#!/usr/bin/python

# Purpose: accept job name to create arguments for run_ingest.py

import sys, os, commands, envvars, datetime, time ,getpass, errno
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
#global return_code, msck_count, msck_command

def call_script(path, job, options):
    
    rc, out = commands.getstatusoutput(kerb)
    print("run_job.py              -> Authenticated    : "+kerb+" RC:"+str(rc))
    cmd = ' '.join(['python',
                    path + '/' + job,
                    options])
    print("run_job.py              -> Invoked    : " + cmd) 
    #rc,status = commands.getstatusoutput(cmd)
    #print status
    call = subprocess.Popen(cmd.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    prev_line = ""
    new_line = 0
    while True:
       line = call.stdout.readline()
       if not line:
           break
       if line.startswith('**ABC_log**'):
          writeabc(line.split("->")[1])
          ref = stdout_file.tell()
       else:
          
          if prev_line != line.strip():
             if new_line !=0:
                print
                new_line=0          
             
             print line.strip()
             ref = stdout_file.tell()
          else:
             if new_line == 0:
                print
             stdout_file.seek(ref)
             print '\r'+'Above status doesnot change and last checked @'+str(datetime.datetime.fromtimestamp(time.time())),
             new_line = 1
          prev_line = line.strip()
    call.communicate()
    return call.returncode
 
def writeabc(line):
    #global msck_count,msck_command, app, sub_app
    if (app.lower() == "bdh" and sub_app.lower() == "data_ingest") or (app.lower() == "apd" and sub_app.lower() == "rreap") :
        with open(abc_log_file, 'w') as myfile:
           myfile.write(line)
        chmod_abc = "chmod 777 "+abc_log_file
        rc, status = commands.getstatusoutput(chmod_abc)
        print "--- running abc_hdfs_put command -->   " + abc_hdfs_put
        rc, status = commands.getstatusoutput(abc_hdfs_put)
        if (rc >0):
           print status
        else:
           print "run_job.py              -> ABC_log written : " + line
           global msck_count
           if (msck_count == 0): 
              print "run_job.py              -> Invoked : " + msck_command
              rc1,status1 = commands.getstatusoutput(msck_command)
              print status1
              msck_count = 1        

def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def main():
    global return_code, msck_count, msck_command
    return_code = 0
    msck_count = 0
    home = '/data/'
    path = os.path.dirname(os.path.realpath(__file__))
    root = path.split('src/scripts')[0]
    
    env = path.split('/')[2].split('bdp')[1]
    env_ver = path.split('/')[4]
    usage = "usage: run_job.py grp_name app sub_app jobnames.list"
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()
    if len(args) < 3:
        parser.error("Arguments - group_job_name and app name are required.")
    global app, sub_app
    grp_name = args[0]
    app = args[1]
    sub_app = args[2]
    jobnames = "jobnames.list"
    
    asofdate = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    rerunjobnames = "jobnames_"+asofdate+".list"
    rerun = "N"
    if len(args) == 4:
       jobnames = args[3].strip()
       rerunjobnames = jobnames
       rerun = "Y"
 
    envvars.populate(env,env_ver,app,sub_app)
        
    log_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')

    log_file = envvars.list['lfs_app_logs'] + "/run_job-" + grp_name + '_' + log_time  + '.log'
    global abc_log_file, stdout_file
    abc_log_file = envvars.list['lfs_app_logs'] + "/"+grp_name+".tmp"
    failed_group_name = "@@"+ grp_name + '_' + log_time 
    
    print("LogFile: " + log_file)
    print("To Kill: kill " + str(os.getpid())) 
    f = open(log_file, "a",0)
    f.close()
    stdout_file = open(log_file, "r+",0)
    sys.stdout = stdout_file
    
    global kerb, user_name 
    rc, user_name = commands.getstatusoutput("echo $USER") 
    
    if env == 'd' or env == 't':
       kerb = "kinit -k -t /home/"+user_name+"/"+user_name.upper()+".keytab "+user_name.upper()+envvars.list['domainName']
    else:
       user_name = envvars.list['srvc_acct_login_'+app+'_'+sub_app]
       kerb = "kinit -k -t /data/bdp"+env+"/"+app+"/keytabs/"+user_name.upper()+".keytab "+user_name.upper()+envvars.list['domainName']
    rc, out = commands.getstatusoutput(kerb)
    print("run_job.py              -> Authenticated    : "+kerb+" RC:"+str(rc))
    
    
    
    
    start_line = "".join('*' for i in range(100))
    print start_line   
    print("run_job.py              -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    global abc_hdfs_put
    hdfs_abc_log_file = envvars.list['hdfs_meta_raw']+"/"+envvars.list['hv_db_meta_stage']+"/abc_hadoop/load_date="+str(asofdate)+"/00000.log";
    abc_hdfs_put = " ".join(["hdfs","dfs","-appendToFile",abc_log_file,
         hdfs_abc_log_file]) 
    hdfs_chmod = "hdfs dfs -chmod -R 777 " + hdfs_abc_log_file 
    rc, out = commands.getstatusoutput(hdfs_chmod)
    print("---Output of chmod command of abc_log_file-->"+hdfs_chmod)

    print("run_job.py              -> Invoked    : " +hdfs_chmod)
    print out 
    #msck_command =  "beeline -u '" + envvars.list['hive2JDBC'] + ";principal=" + envvars.list['hive2Principal']+"' -e "
    msck_command = "hive -e "
    msck_command = msck_command + "'use "+ envvars.list['hv_db_meta_stage']+"; msck repair table abc_hadoop;'"
    
    comments = ""
    # determine joblist file path 
    job_list_file = envvars.list['lfs_app_config'] + '/' + jobnames
    rerun_job_list_file = envvars.list['lfs_app_config'] + '/' + grp_name + "_rerun.list"
    print("run_job.py              -> JobList    : " + job_list_file) 
    
    if os.path.isfile(rerun_job_list_file):
       job_list_file = rerun_job_list_file
       print("run_job.py              -> JobList    : Rerun file found, updating joblist lookup file. Please re-run if original entries has to run.")
       print("run_job.py              -> JobList    : " + job_list_file)
       comments = comments + "Rerun file found "+job_list_file
    else:
       comments = comments + "joblist file " + job_list_file
     
    abc_line = "|".join([grp_name,"run_job.py","python","CA-7 Job","",str(args),"STARTED",
                         user_name,comments.replace(os.linesep,"---"),str(datetime.datetime.today())+"\n"]) 
    writeabc(abc_line)
    input_scripts_count = 0
    failed_scripts_count = 0
    failed_scripts = ""
    try:
        with open(job_list_file) as fin:
            for line in fin:
                args = line.split('|')
                if  args[0].strip().lower() == grp_name.lower() or grp_name.lower() == '*all':
                    options = ' --env ' + env + ' --app ' + app + ' --env_ver ' + env_ver + ' --group ' + grp_name
                    options = options + ' --subapp ' + sub_app
                    if  len(args) < 3:
                        print("Error: Table name and script name not defined in config file")
                        return None, None, None, None, None, None, None
                    
                    if  len(args) >= 4:
                        job = args[2].strip()
                        if args[1].strip().lower() == 'g':
                            path = envvars.list['lfs_global_scripts']
                        else:
                            path = envvars.list['lfs_app_scripts'] 
                        options = options + ' --op0 ' + args[3].strip()
                    if  len(args) >= 5 and args[4].strip != "":
                        options = options + ' --op1 ' + args[4].strip() 
                    if  len(args) >= 6 and args[5].strip != "":
                        options = options + ' --op2 ' + args[5].strip()
                    if  len(args) >= 7 and args[6].strip != "":
                        options = options + ' --op3 ' + args[6].strip()
                    if  len(args) >= 8 and args[7].strip != "":
                        options = options + ' --op4 ' + args[7].strip() 
                    if  len(args) >= 9 and args[8].strip != "":
                        options = options + ' --op5 ' + args[8].strip()
                    if  len(args) >= 10 and args[9].strip != "":
                        options = options + ' --op6 ' + args[9].strip()
                    if  len(args) >= 11 and args[10].strip != "":
                        options = options + ' --op7 ' + args[10].strip() 
                    if  len(args) >= 12 and args[11].strip != "":
                        options = options + ' --op8 ' + args[11].strip()
                    if  len(args) >= 13 and args[12].strip != "":
                        options = options + ' --op9 ' + args[12].strip()
                    input_scripts_count = input_scripts_count + 1
                    rc = call_script(path, job, options)
                    if rc != 0:
                       failed_scripts_count = failed_scripts_count + 1
                       fs = line.split('|')
                       fs[0] = failed_group_name                       
                       failed_scripts = failed_scripts + line
                    if rc > return_code:
                       return_code = rc 


        
    except IOError as e:
        if  e.errno != errno.ENOENT:
            raise IOError("exception file reading error")
        else:
            print("No joblist file found")
    
    if return_code > 0:
       #if input_scripts_count != failed_scripts_count:
          with open(rerun_job_list_file, 'w') as myfile:
             myfile.write(failed_scripts)
          print "run_job.py              -> Failed Script: Some scripts failed.. Please use below command to rerun.."
          print "run_job.py              -> Re-run Cmd   : "+ " ".join(["python",path+"/run_job.py",grp_name,app,sub_app])
          abc_line = "|".join([grp_name,"run_job.py","python","CA-7 Job","",str(args),"FAILED",
                         user_name,"run_job failed, Some scripts failed.." + str(return_code),str(datetime.datetime.today())+"\n"]) 
          writeabc(abc_line)
       #else:
       #   print "run_job.py              -> Failed Script: All scripts failed.. Please use below command to rerun.."
       #   print "run_job.py              -> Re-run Cmd   : "+ " ".join(["python",path+"/run_job.py",grp_name,app,sub_app,jobnames])
       #   abc_line = "|".join([grp_name,"run_job.py","python","CA-7 Job","",str(args),"FAILED",
       #                  user_name,"run_job failed, all scripts failed.." + str(return_code),str(datetime.datetime.today())+"\n"]) 
       #   writeabc(abc_line)
    elif os.path.isfile(rerun_job_list_file):
       print "run_job.py              -> Deleting..." + str(rerun_job_list_file)
       os.remove(rerun_job_list_file)
       
    abc_line = "|".join([grp_name,"run_job.py","python","CA-7 Job","",str(args),"ENDED",
                         user_name,"run_job ended,Return-Code:" + str(return_code),str(datetime.datetime.today())+"\n"]) 
    writeabc(abc_line)
    print("run_job.py              -> Ended      : Return-Code:" + str(return_code)+" " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    silentremove(abc_log_file)
    sys.exit(return_code)

if __name__ == "__main__":
    main()

