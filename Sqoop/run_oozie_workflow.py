#!/usr/bin/python

# Purpose: wrapper for oozie workflow
import os,getpass,envvars,sys
import commands
import time
import datetime
from optparse import OptionParser
def main():
    global return_code, group, start_line
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("run_oozie_workflow.py   -> Started   : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    sys.stdout.flush()
    abc_parameter,workflow, options_file = arg_handle()
    abc_parameters=abc_parameter.split(',')
    env=abc_parameters[0]
    env_ver=abc_parameters[1]
    app=abc_parameters[2]
    sub_app=abc_parameters[3]
    group=abc_parameters[4]
    parent_script=abc_parameters[5]
    #config path for ABC logging
    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app, sub_app)
    print ("ABC Parameter"+abc_parameter)
    oozie_wf_cmd = "oozie job -oozie "+envvars.list['oozieNode']+" -config "
    oozie_wf_cmd = oozie_wf_cmd + options_file
    oozie_wf_cmd = oozie_wf_cmd + ' -Doozie.wf.application.path='
    oozie_wf_cmd = oozie_wf_cmd + workflow
    oozie_wf_cmd = oozie_wf_cmd + ' -debug -run'
    print("run_oozie_workflow.py   -> Invoked   : " + oozie_wf_cmd)
    rc, jobid_str = commands.getstatusoutput(oozie_wf_cmd)
    if rc == 0: 
        jobid_str = jobid_str.split('job: ')
        jobid = jobid_str[1].strip()
        abc_line = "|".join([group,jobid,"oozie","run_oozie_workflow.py","","","STARTED",
                         getpass.getuser(),"oozie workflow started",str(datetime.datetime.today())]) 
        print("**ABC_log**->"+abc_line)
        sys.stdout.flush() 
    else:
        print("run_oozie_workflow.py   -> Failed    : " + jobid_str)
        return_code = 8
        sys.exit(return_code)
    
    print(jobid + "-> Started   : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    sys.stdout.flush()
    #ABC logging
    
    #status = "RUNNING"
    #cnt = 0
    
    get_status(jobid,envvars.list['oozieNode'],"Main")
    abc_line = "|".join([group,jobid,"oozie","run_oozie_workflow.py","","","ENDED",
                         getpass.getuser(),"oozie workflow Ended",str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()
    print(jobid + "-> Ended     : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    
    abc_line = "|".join([group,"run_oozie_workflow.py","python",parent_script,"","","ENDED",
                         getpass.getuser(),"return-code:"+str(return_code),str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush() 
    print("run_oozie_workflow.py   -> Ended     : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
def check_subworkflow(stat_log,oozieNode,oozie_st_cmd,parent_step_name):
    try: 
        print stat_log[len(stat_log)-3] 
        sys.stdout.flush()
        sub_jobid = stat_log[len(stat_log)-3].split()[2].strip()
        final_status = stat_log[4].split(':')[1]
        if "-oozie-oozi-W" in sub_jobid:
           sub_jobid = sub_jobid.split("-oozie-oozi-W")[0] + "-oozie-oozi-W"
           print "Sub workflow:" + sub_jobid + "initiated, checking its status"
           get_status(sub_jobid,oozieNode,"Sub")
           print "Sub workflow:" + sub_jobid + "Completed, checking its parent workflow status"
           # After sub workflow status is verified check the status of current workflow
           rc = 1
           retry_count = 0
           while ( rc!=0 or retry_count <=10):
              rc, stat_log = commands.getstatusoutput(oozie_st_cmd)
              retry_count = retry_count + 1
           if (rc!=0):
              print "Error getting Status, return-code: " + str(rc)
              print stat_log
              sys.exit(11)
           #print "After sub workflow" + stat_log
           stat_log = stat_log.split('\n')
           # check if there is second sub workflow.
           return check_subworkflow(stat_log,oozieNode,oozie_st_cmd,parent_step_name)
        else:
           return stat_log
    except ValueError:
         print('Ignoring: malformed line: "{}"'.format(stat_log))
         get_status(jobid,oozieNode,parent_step_name)
    except IndexError:
         print ('Index error occurred.. Return-code :'+str(rc))
         print "\n".join(stat_log)
         print ('Re-trying..')
         get_status(jobid,oozieNode,parent_step_name)
         
def get_status(jobid,oozieNode,parent_step_name):
    oozie_st_cmd = "oozie job -oozie "+oozieNode+" -info " + jobid
    rc = 1
    retry_count = 0
    while ( rc!=0 or retry_count <=10):
       rc, stat_log = commands.getstatusoutput(oozie_st_cmd)
       retry_count = retry_count + 1
    if rc != 0:
        print "Error getting Status, max retry count 10 reached.. Return-code: " + str(rc)
        print stat_log
        #get_status(jobid,oozieNode,parent_step_name)
        sys.exit(11)  
    try: 
        stat_log = stat_log.split('\n')
        stat_log = check_subworkflow(stat_log,oozieNode,oozie_st_cmd,parent_step_name)

        #last_line = stat_log[len(stat_log)-3].split()
        status = stat_log[len(stat_log)-3][78:88].strip()
        ext_status = stat_log[len(stat_log)-3][111:122].strip()
        ext_rc = stat_log[len(stat_log)-3][122:].strip()
        step_name = stat_log[len(stat_log)-3][:78].split("@")[1]
        #print "Status is " + str(stat_log[len(stat_log)-3][78:88].strip()) 
        if status.strip() == 'RUNNING' or status.strip() == 'PREP':
           
           while (parent_step_name == step_name) and (status.strip() == 'RUNNING' or status.strip() == 'PREP'):
              time.sleep(5)  
              rc = 1
              retry_count = 0
              while ( rc!=0 or retry_count <=10):
                 rc, stat_log = commands.getstatusoutput(oozie_st_cmd)
                 retry_count = retry_count + 1
              if rc == 0:
                 stat_log = stat_log.split('\n')
                 st_count = 0
                 while ( st_count <=10):
                    if stat_log[st_count].split(':')[0].lower().strip() == "status":
                       break
                    st_count = st_count + 1
                 final_status = stat_log[st_count].split(':')[1]
                 print stat_log[len(stat_log)-3]
                 sys.stdout.flush()
                 status = stat_log[len(stat_log)-3][78:88].strip()       
                 ext_status = stat_log[len(stat_log)-3][111:122].strip()  
                 ext_rc = stat_log[len(stat_log)-3][122:].strip()        
                 step_name = stat_log[len(stat_log)-3][:78].split("@")[1]
              else:
                 print "Error getting Status, return-code: " + str(rc)
                 print stat_log
                 sys.exit(11)
           if  (parent_step_name != step_name):
              abc_line = "|".join([group,step_name,"oozie",jobid,"","",status.strip(),
                          getpass.getuser(),"oozie workflow running",str(datetime.datetime.today())]) 
              print("**ABC_log**->"+abc_line)
              sys.stdout.flush()
              get_status(jobid,oozieNode,step_name)
              return
           #return
        rc, stat_log = commands.getstatusoutput(oozie_st_cmd)
        stat_log = stat_log.split('\n')
        print stat_log[len(stat_log)-3] 
        sys.stdout.flush()
        sub_jobid = stat_log[len(stat_log)-3].split()[2].strip()
        st_count = 0
        while ( st_count <=10):
            if stat_log[st_count].split(':')[0].lower().strip() == "status":
               break
            st_count = st_count + 1
        final_status = stat_log[st_count].split(':')[1]
        #It should not reach here with Running status
        if final_status.strip() == 'RUNNING':
           get_status(jobid,oozieNode,parent_step_name)
           return    
        if (ext_status.strip() != 'SUCCEEDED' and ext_rc.strip() != '-' and ext_status.strip() != 'OK') or final_status.strip() != 'SUCCEEDED':
           print "Erroring Since - Ext Status:" + ext_status.strip() + "Ext RC:" + ext_rc.strip()+ "Overall Status:" + final_status.strip()
           print(start_line)
           print "\n".join(stat_log)
           print(start_line)
           abc_line = "|".join([group,step_name,"oozie",jobid,"","",status.strip(),
                   getpass.getuser(),"oozie workflow "+ext_status.strip(),str(datetime.datetime.today())]) 
           print("**ABC_log**->"+abc_line)
           sys.stdout.flush()
           print "Return-Code:9"
           sys.exit(9)
        else:
           abc_line = "|".join([group,step_name,"oozie",jobid,"","",ext_status.strip(),
                       getpass.getuser(),"oozie workflow running",str(datetime.datetime.today())]) 
           print("**ABC_log**->"+abc_line)
           sys.stdout.flush()
         
           #print(jobid + "-> Status    : " + status.strip())
        sys.stdout.flush()
              
        status = stat_log[4].split(':')[1]
    except ValueError:
         print('Ignoring: malformed line: "{}"'.format(stat_log))
         get_status(jobid,oozieNode,parent_step_name)
    except IndexError:
         print ('Index error occurred.. Return-code :'+str(rc))
         print "\n".join(stat_log)
         print ('Re-trying..')
         get_status(jobid,oozieNode,parent_step_name)
    
    return # rc, status
   
    
def arg_handle():
    usage = "usage: run_oozie_workflow.py workflow options_file abc_parameter"

    parser = OptionParser(usage)
    (options, args) = parser.parse_args()
    

    if len(args) != 3:
        abc_line = "|".join(["","run_oozie_workflow.py","python","run_job.py","",str(args),"FAILED",
                         getpass.getuser(),"Argument, workflow path and properties file, is required, return-code:10 ",str(datetime.datetime.today())]) 
        print("**ABC_log**->"+abc_line)
        sys.stdout.flush()
        parser.error("Argument, workflow path and properties file, is required.")
        return_code = 10
        sys.exit(return_code)
    abc_line = "|".join([args[2].split(',')[4],"run_oozie_workflow.py","python",args[2].split(',')[5],"",str(args),"STARTED",
                         getpass.getuser(),"run_oozie_workflow started",str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush() 
    return args[2], args[0],args[1]

if __name__ == "__main__":
    main()
