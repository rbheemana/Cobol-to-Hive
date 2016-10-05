#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time,getpass
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
#sys.path.append('/data/bdpd/bdph/01/global/code/lib')
#from pnc.abc.abclogger import ABCLogger

def main():
    global return_code, group, start_line
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("run_ingest.py           -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    table,transfer_type,field,field_type,field_rdbms_format,field_hadoop_format,lower_bound,mf_file,common_properties,app,sub_app,env,env_ver,group = arg_handle()
    print "field_type=" , field_type
    
    sys.stdout.flush()
    
    # ABC Looging Values
    grp_name = group
    category = 'Ingest'
    tablename = table
    processName = 'run-ingest'
    parentProcessName = 'run-job'
    processType = 'python'
    parameter = 'parameters'
    action = 'STARTED'
    user = getpass.getuser()
    comment = 'Run Oozie workflow'
    asofdate = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    log_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')

    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app, sub_app)
   
    envvars.list[field] = str(lower_bound)
    
    #Get Final Properties final name and path from variables
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + app.replace("/","_") + '_' + table + '.properties'
    

    # Remove if the file exists
    silentremove(final_properties)
    
    # open the final properties file in write append mode
    properties_file = open(final_properties, 'wb')

    # Build the table properties file name and path from variables run_ingest only calls wf_db_ingest workflow
    table_properties = envvars.list['lfs_app_workflows'] + '/wf_mf_'+transfer_type+'_ingest/' + table + '.properties'

    

    #load evironment variables for app specific
    envvars.load_file(table_properties) 
      
    #  Concatenate global properties file and table properties file
    shutil.copyfileobj(open(common_properties, 'rb'), properties_file)
    shutil.copyfileobj(open(table_properties, 'rb'), properties_file)
    
    #Get Databese name from environment variables
    db = envvars.list['hv_db']
    table = envvars.list['hv_table']
    
    # get time stamp to load the table
    hdfs_load_ts = "'" + str(datetime.datetime.now()) +"'"
    
    if field is not None and field.strip() != "":
        print("run_ingest.py           -> DownloadTyp: Partial Download based on where condition ")
        sys.stdout.flush()        
        # Check if the lower_date range is passed from jobnames.list file
        if  lower_bound is None:
            # lower_date range is not found check for presence of exclusions file
            lower_bound,mf_file = get_exception_args(table)
    
            # lower_date is still none get lower date from impala table
            if  lower_bound is None and field is not None and db is not None:
                lower_bound = get_min_bound_impala(db, table, field)
                if  lower_bound is None or lower_bound == "":
                    print("run_ingest.py           -> LowerBound: Cannot be determined.")
                    sys.stdout.flush()
                    sys.exit(2)
                else: 
                    print("run_ingest.py           -> LowerBound: Min date is determined from Impala table") 
            elif lower_bound is None and field is None:
                print("run_ingest.py           -> Arguments error: lower_bound or field or entry in exception file is expected")
                sys.stdout.flush()
                sys.exit(2)
            else:
                print("run_ingest.py           -> LowerBound : Min date is determined from exclusions file") 
                sys.stdout.flush()
        else:
            print("run_ingest.py           -> LowerBound : Min date is determined from jobnames.list file") 
            sys.stdout.flush()
    
        #if  upper_bound is None or upper_bound == "":
        #    upper_bound = str(datetime.datetime.now().date())
        #    print("run_ingest.py           -> UpperBound : Max Date is current date")
        #else:
        #    print("run_ingest.py           -> UpperBound : Max Date source is same as Min date")
        dynamic_properties = '\n'.join(['env=' + env,
                                        'app=' + app,
                                        'happ=' + envvars.list['happ'] ,
                                        field +'='+str(lower_bound),
                                        'partition_column=' + field,
                                        'partition_column_value=' + lower_bound,
                                        'mf_path=' + mf_file,
                                        'hdfs_load_ts=' + hdfs_load_ts  
                       ])

        if field_type.lower() == "int":
            dynamic_properties = dynamic_properties + '\n ' + "where=${partition_column} between ${min_bound} and ${max_bound}" 
            dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=${partition_column} between ${min_bound} and ${max_bound}" 
        elif field_type == None or field_type=="" or field_type.lower() == "date":
            field_rdbms_format=determine_default_field_format(field_rdbms_format) 
            print "field_rdbms_format = ", field_rdbms_format
            sys.stdout.flush()
            field_hadoop_format=determine_default_field_format(field_hadoop_format) 
            print "lower_bound = " , lower_bound
            sys.stdout.flush()
            lower_bound_validated=validate_date_format(lower_bound,field_rdbms_format)
            #upper_bound_validated=validate_date_format(upper_bound,field_rdbms_format)
            print lower_bound_validated
            sys.stdout.flush()
            #print upper_bound_validated
            dynamic_properties = dynamic_properties + '\n ' + "where=${partition_column} between '${min_bound}' and '${max_bound}'" 
            lower_bound_hadoop=lower_bound_validated.strftime(field_hadoop_format)
            #upper_bound_hadoop=upper_bound_validated.strftime(field_hadoop_format)
            dynamic_properties = dynamic_properties + '\n' + \
                                   '\n'.join([ 'min_bound_hadoop=' + lower_bound_hadoop
             #              'max_bound_hadoop=' + upper_bound_hadoop
                       ])

            dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=${partition_column} between '${min_bound_hadoop}' and '${max_bound_hadoop}'" 
            abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table+','+field+ lower_bound_hadoop +"to"+upper_bound_hadoop       
    else:
        print("run_ingest.py           -> DownloadTyp: Full Download of table ")
        sys.stdout.flush()
        dynamic_properties = '\n'.join(['env=' + env,
                                        'app=' + app,
                                        'sub_app='+sub_app,
                                        'group=' +group,
                                        'happ=' + envvars.list['happ'] ,
                                        #field +'='+str(lower_bound),
                                        'min_bound=' + "''",
                                        'max_bound=' + "''",
                                        'min_bound_hadoop=' + "''",
                                        'max_bound_hadoop=' + "''",
                                        'mf_path=' + mf_file,
                                        'hdfs_load_ts=' + hdfs_load_ts  
                                       ])
        dynamic_properties = dynamic_properties + '\n ' + "where=1=1"
        dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=1=1"
        abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table +","
    
    if transfer_type.strip().lower() == "bin":
       try:
          if (envvars.list['override_hql'] is not None and envvars.list['override_hql'] != ''):
             script_name = envvars.list['override_hql'].strip()
             hdfs_copy = "hdfs dfs -put -f " + envvars.list['lfs_app_workflows'] +  '/wf_mf_'+transfer_type+'_ingest/' + script_name + " " + envvars.list['hdfs_app_workflows'] + '/wf_mf_'+transfer_type+'_ingest'
             print("run_mf_ingest.py    -> Invoked   : " + hdfs_copy)
             rc,status = commands.getstatusoutput(hdfs_copy)
             if rc > return_code:
                return_code = rc
          else:
            script_name = 'hv_ins_stg_fnl.hql'
       except KeyError:
          script_name = 'hv_ins_stg_fnl.hql'
    dynamic_properties = dynamic_properties + '\n ' + "script_name="+script_name
                    
    #ABC logging parameter for oozie
    #print "env"+ env
    sys.stdout.flush()
    #abc_parameter = env+','+env_ver+','+app+','+group+","+table+','+field+ lower_bound_hadoop +"to" #+upper_bound_hadoop
    

    properties_file.write(dynamic_properties)
    properties_file.close()
    print("run_ingest.py           -> CommnPrpty : " + common_properties) 
    print("run_ingest.py           -> TablePrpty : " + table_properties)
    print("run_ingest.py           -> DynmcPrpty : " + dynamic_properties.replace("\n",", ")) 
    print("run_ingest.py           -> FinalPrpty : " + final_properties) 
    sys.stdout.flush()
     # ABC Logging Started
    parameter_string=""
    if lower_bound is not None and lower_bound != "":
        parameter_string = field +" "+lower_bound+ " "+upper_bound
    comments =  "Properties file name :" +final_properties
    abc_line = "|".join([group,"run_ingest.py","python","run_job.py",str(table),parameter_string,"RUNNING",
                         getpass.getuser(),comments,str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+",run_ingest.py"  
    sys.stdout.flush()
    #config path for ABC logging
    config_folder = envvars.list['lfs_global_config'] 
    #config_path = config_folder + "/abclogger_config.properties" 
    log_file_path = envvars.list['lfs_app_logs']

    #aa = ABCLogger(config_path,log_file_path)
    #call_ABCLogger(aa,app,asofdate,category,grp_name,tablename.upper(),processName,parentProcessName,processType,parameter_string,action,log_time,user,comment)

    rc = runoozieworkflow(final_properties,abc_parameter,transfer_type)
    print "Return-Code:" + str(rc)
    if rc > return_code:
       return_code = rc
    
    action = 'ENDED'
    log_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
    asofdate = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    #call_ABCLogger(aa,app,asofdate,category,grp_name,tablename.upper(),processName,parentProcessName,processType,parameter_string,action,log_time,user,comment)
    #aa.close();

    print("run_ingest.py           -> Ended      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    sys.stdout.flush()
    sys.exit(return_code)

    
def runoozieworkflow(final_properties,abc_parameter,transfer_type):    
    #command to trigger oozie script
    workflow = envvars.list['hdfs_app_workflows'] + '/wf_mf_'+transfer_type+'_ingest'
    oozie_wf_script = "python " + envvars.list['lfs_global_scripts'] + "/run_oozie_workflow.py " + workflow + ' ' + final_properties +' '+abc_parameter

    print("run_ingest.py           -> Invoked   : " + oozie_wf_script) 
    #rc,status = commands.getstatusoutput(oozie_wf_script)
    #print(status)
    call = subprocess.Popen(oozie_wf_script.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    while True:
       line = call.stdout.readline()
       if not line:
           break
       print line.strip()
       sys.stdout.flush()
    call.communicate()
    print "call returned"+str(call.returncode) 
    return call.returncode
    


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def determine_default_field_format(field_format):
    print "field_format = ", field_format
    sys.stdout.flush()
    if field_format==None or field_format.strip()=="":
       field_format="%Y-%m-%d"
    return field_format

def arg_handle():
    usage = "usage: run_ingest.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-f", "--op2", dest="field",
                      help="increment field")
    parser.add_option("-l", "--op3", dest="lower_bound",
                      help="date to which the file download")
    parser.add_option("-u", "--op4", dest="mf_file",
                      help="mainframe file name") 
    parser.add_option("-w", "--op1", dest="tran_typ",
                      help="mainframe file name")
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-b", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-t", "--op0", dest="table",
              help="environment name")
    parser.add_option("-g", "--group", dest="group",
              help="environment name")

    (options, args) = parser.parse_args()
    print("run_ingest.py           -> Input      : " + str(options)) 
    sys.stdout.flush()
    if  options.table == "":
        parser.error("Argument, table_name, is required.")
    table = options.table.lower()
    field = options.field
    field_name_type_fmt = None
    field_name = None
    field_type = None
    field_rdbms_format=None
    field_hadoop_format=None
    field_delimiter = "#"
    print "field = ", field
    sys.stdout.flush()
    if field is not None and field.strip() != "":
       field_name_type_fmt=field.split(field_delimiter)
       print "field_name_type_fmt = ", field_name_type_fmt
       sys.stdout.flush()
       field_name=field_name_type_fmt[0]
       field_type=""
       print "len field_name_type_fmt = ", len(field_name_type_fmt)
       sys.stdout.flush()
       if len(field_name_type_fmt) >=2:
          field_type=field_name_type_fmt[1]
       if len(field_name_type_fmt) >=3:
          field_rdbms_format=field_name_type_fmt[2]
       if len(field_name_type_fmt) >=4:
          field_hadoop_format=field_name_type_fmt[3]
    source = '/data/bdp' + options.env + '/bdh/' + options.env_ver + '/global/config/oozie_global.properties'
    
    group = options.group
    return table,options.tran_typ.strip().lower(),field_name,field_type,field_rdbms_format,field_hadoop_format,options.lower_bound,options.mf_file,source,options.app,options.sub_app,options.env,options.env_ver,group


def get_exception_args(table_name):
    try:
        with open('exception.properties') as fin:
            for line in fin:
                args = line.split(':')
                if  args[0].strip().lower() == table_name:
                    if  len(args) < 2:
                        print("lower_bound: not defined in exception file")
                        return None, None
                    if  len(args) == 2:
                        return args[1].strip(), args[1].strip()
                    else: 
                        return args[1].strip(), args[2].strip()
        return None, None
    except IOError as e:
        if  e.errno != errno.ENOENT:
            raise IOError("exception file reading error")
        else:
            return None, None 


def get_min_bound_impala(db_name, table_name,field):
    impala_cmd = envvars.list['impalaConnect']+ ' "invalidate metadata ' + db_name + '.' + table_name + '; select Max(' + field + ') from ' + db_name + '.' + table_name + '"'
    rc, output = commands.getstatusoutput(impala_cmd)
    outputlist = output.split('\n')
    if  rc == 0:
        max_date_str = outputlist[-1].strip()
        validate_date(max_date_str)
        min_bound = datetime.datetime.strptime(max_date_str, "%Y-%m-%d") + datetime.timedelta(days=1) 
        min_bound =min_bound.date()
        return str(min_bound)
    else:
        return None

def validate_ped(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y%m')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYYMM/YYYY-MM-DD but is " + date_text)
        
def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        validate_date_format(date_text,"%d-%b-%Y")

def validate_date_format(date_text,dateFormat):
    print "validate_date_format():: ", date_text, " dateFormat=" , dateFormat
    sys.stdout.flush()
    try:
        return datetime.datetime.strptime(date_text, dateFormat)
    except ValueError:
        validate_ped(date_text)

if __name__ == "__main__":
    main()
