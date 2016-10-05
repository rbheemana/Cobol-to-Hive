#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time,getpass
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE

def main():
    global return_code
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("run_ingest.py           -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    table,field,field_type,field_rdbms_format,field_hadoop_format,lower_bound,upper_bound,common_properties,app,sub_app,env,env_ver,group = arg_handle()
    print "field_type=" , field_type
    

    # Get envvars from oozie_common_properties file
    envvars.populate(env,env_ver,app,sub_app)   
    
    #Get Final Properties final name and path from variables
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + app.replace("/","_") + '_' + table + '.properties'
    

    # Remove if the file exists
    silentremove(final_properties)
    
    # open the final properties file in write append mode
    properties_file = open(final_properties, 'wb')

    # Build the table properties file name and path from variables run_ingest only calls wf_db_ingest workflow
    table_properties = envvars.list['lfs_app_workflows'] + '/wf_db_ingest/' + table + '.properties'


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
    sys.stdout.flush()
    if field is not None:
        print("run_ingest.py           -> DownloadTyp: Partial Download based on where condition ")        
        # Check if the lower_date range is passed from jobnames.list file
        if  lower_bound is None or lower_bound == "":
            # lower_date range is not found check for presence of exclusions file
            lower_bound,upper_bound = get_exception_args(envvars.list['lfs_app_config'],table)
    
            # lower_date is still none get lower date from impala table
            if  lower_bound is None and field is not None and db is not None:
                lower_bound = get_min_bound_impala(db, table, field,field_type)
                if  lower_bound is None or lower_bound == "":
                    print("run_ingest.py           -> LowerBound: Cannot be determined.")
                    return_code = 2
                    sys.exit(return_code)
                else: 
                    print("run_ingest.py           -> LowerBound: Min date is determined from Impala table") 
            elif lower_bound is None and field is None:
                print("run_ingest.py           -> Arguments error: lower_bound or field or entry in exception file is expected")
                return_code = 2
                sys.exit(return_code)
            else:
                print("run_ingest.py           -> LowerBound : Min date is determined from exclusions file") 
        else:
            print("run_ingest.py           -> LowerBound : Min date is determined from jobnames.list file") 
    
        if  upper_bound is None or upper_bound == "":
            curr_dt = str(datetime.datetime.now().date())
            if field.strip().lower() == "msrmnt_prd_id":
               print "run_ingest.py           -> Upper_bound      : BDW table date used "+str(curr_dt)
               upper_bound = get_bdw_date_from_id(db, curr_dt)
            else:
               upper_bound = curr_dt
               print("run_ingest.py           -> UpperBound : Max Date is current date")
        else:
            print("run_ingest.py           -> UpperBound : Max Date source is same as Min date")
        dynamic_properties = '\n'.join(['env=' + env,
                                        'app=' + app,
                                        'sub_app=' +sub_app,
                                        'group=' +group,
                                        'happ=' + envvars.list['happ'] ,
                                        'min_bound=' + lower_bound,
                                        'max_bound=' + upper_bound,
                                        'hdfs_load_ts=' + hdfs_load_ts])

        if field_type.lower() == "int":
            dynamic_properties = dynamic_properties + '\n ' + "where=${partition_column} between ${min_bound} and ${max_bound}" 
            dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=${partition_column} between ${min_bound} and ${max_bound}" 
            abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table+','+field+ lower_bound+"to"+upper_bound
        elif field_type == None or field_type=="" or field_type.lower() == "date":
            field_rdbms_format=determine_default_field_format(field_rdbms_format) 
            #print "field_rdbms_format = ", field_rdbms_format
            field_hadoop_format=determine_default_field_format(field_hadoop_format) 
            #print "lower_bound = " , lower_bound
            lower_bound_validated=validate_date_format(lower_bound,field_rdbms_format)
            upper_bound_validated=validate_date_format(upper_bound,field_rdbms_format)
            #print lower_bound_validated
            #print upper_bound_validated
            dynamic_properties = dynamic_properties + '\n ' + "where=${partition_column} between '${min_bound}' and '${max_bound}'" 
            lower_bound_hadoop=lower_bound_validated.strftime(field_hadoop_format)
            upper_bound_hadoop=upper_bound_validated.strftime(field_hadoop_format)
            dynamic_properties = dynamic_properties + '\n' + \
                                   '\n'.join([ 'min_bound_hadoop=' + lower_bound_hadoop,
                          'max_bound_hadoop=' + upper_bound_hadoop
                       ])

            dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=${partition_column} between '${min_bound_hadoop}' and '${max_bound_hadoop}'" 
            abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table+','+field+ lower_bound_hadoop +"to"+upper_bound_hadoop    
    else:
        print("run_ingest.py           -> DownloadTyp: Full Download of table ")
        dynamic_properties = '\n'.join(['env=' + env,
                                        'app=' + app,
                                        'sub_app=' +sub_app,
                                        'group=' +group,
                                        'happ=' + envvars.list['happ'] ,
                                        'min_bound=' + "''",
                                        'max_bound=' + "''",
                                        'min_bound_hadoop=' + "''",
                                        'max_bound_hadoop=' + "''",
                                        'hdfs_load_ts=' + hdfs_load_ts])
        dynamic_properties = dynamic_properties + '\n ' + "where=1=1"
        dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=1=1"
        abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table +"," 
                    
    #ABC logging parameter for oozie
    #print "env"+ env
    #abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table+','+field+ lower_bound_hadoop +"to"+upper_bound_hadoop
    

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
    rc = runoozieworkflow(final_properties,abc_parameter)
    print "Return-Code:" + str(rc)
    if rc > return_code:
       return_code = rc
    abc_line = "|".join([group,"run_ingest.py","python","run_job.py",str(table),parameter_string,"ENDED",
                         getpass.getuser(),"return-code:"+str(return_code),str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()                     

    print("run_ingest.py           -> Ended      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    print "Return-Code:" + str(return_code)
    sys.exit(return_code)
  
def runoozieworkflow(final_properties,abc_parameter):    
    #command to trigger oozie script
    workflow = envvars.list['hdfs_app_workflows'] + '/wf_db_ingest'
    oozie_wf_script = "python " + envvars.list['lfs_global_scripts'] + "/run_oozie_workflow.py " + workflow + ' ' + final_properties +' '+abc_parameter

    print("run_ingest.py           -> Invoked   : " + oozie_wf_script) 
    sys.stdout.flush()
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
    #print "field_format = ", field_format
    if field_format==None or field_format.strip()=="":
       field_format="%Y-%m-%d"
    return field_format

def arg_handle():
    usage = "usage: run_ingest.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-f", "--op1", dest="field",
                      help="increment field")
    parser.add_option("-l", "--op2", dest="lower_bound",
                      help="increment field min bound")
    parser.add_option("-u", "--op3", dest="upper_bound",
                      help="increment field max bound") 
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
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
    if  options.table == "":
        parser.error("Argument, table_name, is required.")
        return_code = 10
        sys.exit(return_code)
    table = options.table.lower()
    field = options.field
    field_name_type_fmt = None
    field_name = None
    field_type = None
    field_rdbms_format=None
    field_hadoop_format=None
    field_delimiter = "#"
    #print "field = ", field
    if field is not None:
       field_name_type_fmt=field.split(field_delimiter)
       #print "field_name_type_fmt = ", field_name_type_fmt
       field_name=field_name_type_fmt[0]
       field_type=""
       #print "len field_name_type_fmt = ", len(field_name_type_fmt)
       if len(field_name_type_fmt) >=2:
          field_type=field_name_type_fmt[1]
       if len(field_name_type_fmt) >=3:
          field_rdbms_format=field_name_type_fmt[2]
       if len(field_name_type_fmt) >=4:
          field_hadoop_format=field_name_type_fmt[3]
    source = '/data/bdp' + options.env + '/bdh/' + options.env_ver + '/global/config/oozie_global.properties'
    
    group = options.group
    abc_line = "|".join([group,"run_ingest.py","python","run_job.py",str(table),str(options),"STARTED",
                         getpass.getuser(),"run_ingest started..",str(datetime.datetime.today())]) 
    print("**ABC_log**->"+abc_line)
    sys.stdout.flush()
    return table,field_name,field_type,field_rdbms_format,field_hadoop_format,options.lower_bound,options.upper_bound,source,options.app,options.sub_app,options.env,options.env_ver,group


def get_exception_args(app_config_folder,table_name):
    try:
        with open(app_config_folder+'/exception.properties') as fin:
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
            return_code = 10
            raise IOError("exception file reading error")
            sys.exit(return_code)
        else:
            return None, None 

def get_bdw_date_from_id(db_name,curr_dt):
    impala_cmd = envvars.list['impalaConnect'] +' "invalidate metadata ' + db_name + '.msrmnt_prd;select msrmnt_prd_id from '+db_name + ".msrmnt_prd where msrmnt_prd_dt = '"+curr_dt+"'"+';"'
    rc, output = commands.getstatusoutput(impala_cmd)
    outputlist = output.split('\n')
    if  rc == 0:
        max_date_str = outputlist[-1].strip()
        validate_int(max_date_str)
        return max_date_str   
    else:
        print "run_ingest.py           -> ERROR      : " + db_name + ".msrmnt_prd table needs to be sqooped first before determining the current partition value for BDW tables"
        sys.exit(9) 
        return None
def get_bdw_id_from_date(db_name,prd_id):
    impala_cmd = envvars.list['impalaConnect'] +' "invalidate metadata ' + db_name + '.msrmnt_prd;select msrmnt_prd_dt from '+db_name + ".msrmnt_prd where msrmnt_prd_id = "+prd_id+';"'
    rc, output = commands.getstatusoutput(impala_cmd)
    outputlist = output.split('\n')
    if  rc == 0:
        max_date_str = outputlist[-1].strip()
        validate_date(max_date_str)
        return max_date_str   
    else:
        print "run_ingest.py           -> ERROR      : " + db_name + ".msrmnt_prd table needs to be sqooped first before determining the current partition value for BDW tables"
        sys.exit(9) 
        return None


def get_min_bound_impala(db_name, table_name,field,field_type):
    impala_cmd = envvars.list['impalaConnect'] +' "invalidate metadata ' + db_name + '.' + table_name + '; select Max(' + field + ') from ' + db_name + '.' + table_name + '"'
    rc, output = commands.getstatusoutput(impala_cmd)
    outputlist = output.split('\n')
    if  rc == 0:
        max_date_str = outputlist[-1].strip()
        if field_type == 'int' and max_date_str == "":
           max_date_str = "0"
           return max_date_str
        elif field_type == "" and max_date_str =="":
           max_date_str = "1900-01-01"
           return max_date_str
        if field.strip().lower() == "msrmnt_prd_id":
           min_bound = datetime.datetime.strptime(get_bdw_id_from_date(db_name, max_date_str), "%Y-%m-%d") + datetime.timedelta(days=1) 
           print "run_ingest.py           -> Lower_bound      : BDW table date used "+str(min_bound.date())
           return get_bdw_date_from_id(db_name, str(min_bound.date()))
       
        if field_type == 'int':
           validate_int(max_date_str)
           return max_date_str   
        else:   
           validate_date(max_date_str)
           min_bound = datetime.datetime.strptime(max_date_str, "%Y-%m-%d") + datetime.timedelta(days=1) 
           min_bound =min_bound.date()
           return str(min_bound)
    else:
        return None

def validate_int(int_text):
    try:
        num=int(int_text)
    except:
        return_code = 10
        raise ValueError("Incorrect data format, should be int but is " + max_date_str)
        sys.exit(return_code)

def validate_ped(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y%m')
    except ValueError:
        return_code = 10
        raise ValueError("Incorrect data format, should be YYYYMM/YYYY-MM-DD but is " + date_text)
        sys.exit(return_code)
        
def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        validate_date_format(date_text,"%d-%b-%Y")

def validate_date_format(date_text,dateFormat):
    #print "validate_date_format():: ", date_text, " dateFormat=" , dateFormat
    try:
        return datetime.datetime.strptime(date_text, dateFormat)
    except ValueError:
        validate_ped(date_text)

if __name__ == "__main__":
    main()
