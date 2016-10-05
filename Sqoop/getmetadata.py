#!/usr/bin/python

import sys,os,time, envvars, shutil, errno
import ConfigParser
import datatypes, re
import optparse , commands

import subprocess                   
from subprocess import Popen, PIPE 

optionusage_small = sys.argv[0] + " -e <env d/t/q/p> -v <env_ver 01/02> -s <filepath> "
optionusage_expanded= sys.argv[0] + " -env <env d/t/q/p> -env_ver <env_ver 01/02> --sparamfile <filepath> "
def usage():
    print "getmetadata.py           -> usage:  "
    print "getmetadata.py           ->         ", optionusage_small
    print "getmetadata.py           ->         ", optionusage_expanded

def checkenv(env):
    env_root="/data/bdp"+env
    try:
       env_root = os.environ['ENV_ROOT']
    except KeyError:
       print "getmetadata.py           -> using env_root = ", env_root
       if env_root == None or env_root.strip() == '':
           print "getmetadata.py           -> ENV_ROOT is not set. So defaulting to " , env_root
    return env_root    
def silentremovedir(dirname):
    try:
        shutil.rmtree(dirname)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured


def get_keystore_root(key_store,app,user_id):
    keystore_root=""
    if key_store is None or key_store.lower() == "service" :
        keystore_root = envvars.list['hdfs_service_keystore_root'] + "/" + app
    elif key_store.lower() == "common" :
        keystore_root = envvars.list['hdfs_common_keystore_root']
    elif key_store.lower() == "user" :
        keystore_root = "/user/" + user_id.lower() 
    else:
        keystore_root = "/user/" + os.environ['USER']

    if keystore_root is None:
        print "getmetadata.py           -> keystore_root is None. So setting default"
        keystore_root = "/user/" + os.environ['USER']

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

def validateOptions():
    parser.add_option('-e', '--env', dest='env', help='Sqoop env [d/q/p/t] properties to use.')
    parser.add_option('-v', '--env_ver', dest='env_ver', help='Sqoop env version[01/02]  to use.') 
    parser.add_option('-a', '--app', dest='app', help='application')
    parser.add_option('-u', '--subapp', dest='sub_app', help='Sub application')
    parser.add_option('-s', '--sparamfile', dest='sqoopparams', help='Sqoop Param Config File.')
    parser.add_option('-t', '--tableName', dest='tableName', help='Sqoop Table for metadata.')
    parser.add_option('-k', '--key_store', dest='key_store', help='key store is for service id or db user id.')
    
    (options, args) = parser.parse_args()
    print "getmetadata.py           -> Input:"+str(options)
    if options.env is None:
       usage()
       sys.exit(1)

    if options.env_ver is None:
       usage()
       sys.exit(1)
    
    if options.sqoopparams is None:
       usage()
       sys.exit(1)

    if options.key_store is None:
       usage()
       sys.exit(1)

    #if options.tableName is None:
    #   print "getmetadata.py           -> Info: All tables passed in " + options.sqoopparams + " will be used"
    
    return options   

def getOutputFilePath(env_ver,appName,sub_app,sqoopParamFilePath):
    jobOutputDir="generated/"+appName+"/" + env_ver + "/" + sub_app + "/"
    print "getmetadata.py           -> jobOutputDir = ", jobOutputDir
    if not os.path.exists(jobOutputDir):
       os.makedirs(jobOutputDir)
    folder,outFileName=os.path.split(sqoopParamFilePath)
    metadataFileName = outFileName + ".meta"
    return jobOutputDir + metadataFileName

def getTableNames( tableItems):
    #tableItem will be a tuple as ( tableName, tableSqoopParams )
    tablesToSqoop=""
    seperator=""
    for tableItem in tableItems:
       tableName = tableItem[0]
       tablesToSqoop = tablesToSqoop + seperator + "'"+ tableItem[0] + "'"
       seperator=","
    return tablesToSqoop

def getMetadataQuery(global_config,datasource,database,tableNames):
    query=""
    for line in open(global_config+"/"+ datasource + "_METADATA_Query.sqoop"):
         query = query + " " + line.strip()
    query = query.replace("${databaseNameToken}",database.upper())
    query= query.replace("${tableNamesToken}",tableNames.upper())      
    #print "getmetadata.py           -> query = " + query
    return query

def sqoopMetadata(global_config,appName,datasource,database,jdbcUrl,integrated_security,userName,passwordAlias,passwordKeyProvider,tablesToSqoop,outputFilePath):
    query = getMetadataQuery(global_config,datasource,database,tablesToSqoop)
    targetDir = "temp/sqoop/datasource/"
    status = os.system("hdfs dfs -rm -r " + targetDir)

    sqoopParams="sqoop^import^-Dhadoop.security.credential.provider.path=" + passwordKeyProvider + \
                 "^-Doozie.sqoop.log.level=ERROR" \
                 "^--connect^\"" + jdbcUrl + \
                 "\"^--query^\"" + query + "\"" \
                 "^--m^1^" + \
                 "--target-dir^" + targetDir
    if integrated_security is not None and not integrated_security.strip().lower() == "true":
         sqoopParams = sqoopParams + \
                      "^--username^" + userName + \
                      "^--password-alias^\"" + passwordAlias + "\"" 


    print("getmetadata.py           -> Invoked   : " + " ".join(sqoopParams.split('^'))) 
    rc, status = commands.getstatusoutput(" ".join(sqoopParams.split('^')))  
    #while True:
    #   line = call.stdout.readline()
    #   if not line:
    #       break
    #   print line.strip()
    #   sys.stdout.flush()
    #call.communicate()
    #status=call.returncode
    print status
    print "getmetadata.py           -> sqoop status = ", rc
    if rc != 0:
         print "getmetadata.py           -> Getting Metadata failed..."
         sys.exit(1)

    silentremovedir(outputFilePath)
    #os.system( "rm " + outputFilePath);
    os.system( "hdfs dfs -get " + targetDir + "/part-m-00000 " + outputFilePath)
    os.system( "chmod 777 " + outputFilePath) 
    print("getmetadata.py           -> MetadataLocation : " + outputFilePath) 


if __name__ == "__main__":
     parser = optparse.OptionParser()
     options = validateOptions()

     env = options.env
     env_ver = options.env_ver
     env_root = checkenv(options.env)
     
     common_root_path= env_root + "/bdh/01/global"

     sys.path.append( os.path.expanduser(common_root_path + "/code/scripts/") )
     import envvars
     envvars.populate(env,env_ver,options.app,options.sub_app)
     sqoopParamFilePath = envvars.list['lfs_app_config']+"/"+options.sqoopparams
     tableName = options.tableName
     print "getmetadata.py           -> sqoopParamFilePath = " + sqoopParamFilePath
     
     config = ConfigParser.ConfigParser()
     config.readfp(open(sqoopParamFilePath))


     appName = config.get("DataSourceInfo","app")
     app = appName
     sub_app = config.get("DataSourceInfo","sub_app")
     if appName.find("/") != -1:
        app = appName.split("/")[0]
        sub_app = appName.split("/")[1]
     

     
     datasource = config.get("DataSourceInfo","datasource")
     database = config.get("InfoForMetadata","database").upper()
     jdbc_ref = config.get("InfoForMetadata","jdbc_ref").lower()
     db_user_id = config.get("InfoForMetadata","db_user_id").upper()
     integrated_security = config.get("InfoForMetadata","integrated_security",0,{'integrated_security':'false'})

     
     jdbcUrl = envvars.list[datasource+"_jdbc_connect_" + jdbc_ref]
     if options.key_store == "service":
        print "getmetadata.py           -> getting service id from environment...." + datasource+"_username_" + jdbc_ref
        userName = envvars.list[datasource+"_username_" + jdbc_ref]
     else:
        userName = db_user_id
     
     passwordAlias= get_alias_name(options.key_store,jdbc_ref,userName)

     #passwordKeyProvider = envvars.list[datasource+ "_password_key_provider"]
     passwordKeyProvider = "jceks://hdfs" + get_keystore_root(options.key_store, app,db_user_id) \
                                          + "/" + userName.upper() + ".jceks"

     #print "getmetadata.py           -> JDBC_URL = " + jdbcUrl
     #print "getmetadata.py           -> userName = " + userName
     #print "getmetadata.py           -> password_alias = " + passwordAlias
     #print "getmetadata.py           -> password_key_provider =" + passwordKeyProvider
    
     tablesToSqoop=""
     if not tableName:
        tableItems = config.items("TableInfo")
        tablesToSqoop = getTableNames( tableItems)
     else:
        tablesToSqoop=tableName

     if len(tablesToSqoop) <= 0:
         print "getmetadata.py           -> TableName not given or Table configuration with 'TableInfo' tag could not be found in ", sqoopParamFilePath
         sys.exit(1)
     outputFile = envvars.list['lfs_app_config']+"/"+options.sqoopparams+".meta"    
     sqoopMetadata(envvars.list['lfs_global_config'],appName,datasource,database,jdbcUrl,integrated_security,userName,passwordAlias,passwordKeyProvider,tablesToSqoop, outputFile)

