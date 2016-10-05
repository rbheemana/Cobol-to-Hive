###################################################################################################
#   This program generates hive scripts based on the 
#   metadata path sent as parameter
#
#   usage: python generate.py --sparamfile --metadatapath --table --workflowpath
#            --sparamfile   : Properties file database to connect, tables to sqoop etc
#            --metadatapath : Path to the metadata for the tables mentioned in above properties file
#            --table        : Table to sqoop from the specified properties file. Not implemented
#            --workflowpath : 
#
###################################################################################################

#!/usr/bin/python

import sys,os,time
import ConfigParser
import datatypes, re
import optparse 
import subprocess 
import sys, os, commands

optionusage_small = sys.argv[0] + " -s <filepath> -m <metadatafilepath> [-t <tablename>] -w <workflow location>"
optionusage_expanded= sys.argv[0] + " --sparamfile <filepath> --metadatapath  <metadatafilepath> [--table <tablename>] --workflowpath <db_ingest workflow path>"

#Print the command usage
def usage():
    print "generate.py             -> usage:  "
    print "generate.py             ->         ", optionusage_small
    print "generate.py             ->         ", optionusage_expanded
    

#This function is to validate and parse the input options passed to this module
def validateOptions():
    parser.add_option('-s', '--sparamfile', dest='sqoopparams', help='Sqoop Param Config File')
    parser.add_option('-m', '--metadatapath', dest='metadatapath', help='Metadata file for tables')
    parser.add_option('-t', '--table', dest='tableName', help='Table name to sqoop')
    parser.add_option('-w', '--workflowpath', dest='workflowpath', help='Generic workflow path to copy generated scripts')
    parser.add_option('-k', '--key_store', dest='key_store', help='Key store to refer service user id or individual id')
    parser.add_option('-a', '--app', dest='app', help='application')
    parser.add_option('-u', '--subapp', dest='sub_app', help='Sub application')
    parser.add_option('-v', '--env_ver', dest='env_ver', help='Environment version')
    parser.add_option('-e', '--env', dest='env', help='Environment')
    
    (options, args) = parser.parse_args()
    if options.sqoopparams is None:
       usage()
       sys.exit(1)
    
    if options.metadatapath is None:
       usage()
       sys.exit(1)

    if options.tableName is None:
       print "generate.py             -> Info: All tables passed in " + options.sqoopparams + " will be used"
    
    if options.workflowpath is None:
       print "generate.py             -> Info: Generated files will not be copied to Workflow folder."

    if options.key_store is None:
       print "generate.py             -> Info: key_store is not present. Please specify key_store"
       sys.exit(1)
    if options.env_ver is None:
       print "generate.py             -> Info: env_ver is not present. Please specify env_ver."
       sys.exit(1)
    return options   

#Gets the path to create the output file from this program
def getOutputFilePath(env_ver, appName,sub_app,sqoopParamFilePath,ext):
    jobOutputDir="generated/"+appName+"/" + env_ver + "/" + sub_app + "/"
    if not os.path.exists(jobOutputDir):
       os.makedirs(jobOutputDir)
    folder,outFileName=os.path.split(sqoopParamFilePath)
    fileName = outFileName + ext
    return jobOutputDir + fileName

#Populate properties file to be used for data ingest workflow and hive create scripts
def generateScripts(template_path, app_src_path, app_wrk_path,datasource,sourceSystem, jdbcReference, dbUserName,env_ver, appName,sub_app, tableItems, sqoopParamFilePath, sqoopparams,key_store,database):

        tableFound="false"
        jobTemplateText=""   
        
        wfPath = getWorkflowPath()
        if wfPath is None:
            print "generate.py             -> Generated files are not copied to workflow folder"
        elif wfPath == "":
            print "generate.py             -> Generated files are not copied to workflow folder"
        else:
            wfPath = getWorkflowPath()
            print "generate.py             -> Generated properties will be copied to " + wfPath
         
        
        
        
        #Read job.properties.template
        print ("generate.py             -> Reading job.properties.template......")
        for line in open(template_path+"/templates/job.properties.template"):
            jobTemplateText = jobTemplateText + line

        createStgTemplateText=""
        print ("generate.py             -> Reading create_stg.hql......")
        for line in open(template_path+"/templates/create_stg.hql"):
            createStgTemplateText = createStgTemplateText + line

        createParquetTemplateText=""
        print ("generate.py             -> Reading create_parquet......")
        for line in open(template_path+"/templates/create_parquet.hql"):
            createParquetTemplateText = createParquetTemplateText + line


        #jobOutputDir="generated/"+appName+"/" +env_ver + "/" + sub_app
        #if not os.path.exists(jobOutputDir):
        #    os.makedirs(jobOutputDir)
        #    os.chmod(jobOutputDir,0777)
        
        tablesGenerated=""
        #tableItem will be a tuple as ( tableName, tableSqoopParams )
        for tableItem in tableItems:

            tableName = tableItem[0]
            tableInfo = tableItem[1]
            tableParams = tableInfo.split(",")
            paramsLength = len(tableParams)
            #scriptOutputDir="generated/"+appName+"/" + env_ver + "/" +sub_app + tableName.lower()
            #scriptOutputDir=jobOutputDir +"/" + tableName.lower()
            #if not os.path.exists(scriptOutputDir):
            #    os.makedirs(scriptOutputDir)
            #    os.chmod(scriptOutputDir,0777)

            #Read sqoop properties of a table [from input properties file]
            partitionCol =tableParams[0].strip() if paramsLength > 0 else ""
            splitBy      =tableParams[1].strip() if paramsLength > 1 else ""
            numMappers   =tableParams[2].strip() if paramsLength > 2 else 4

            #Target hive table name if hive table name needs to be different from source table name
            hiveTableName=tableParams[3] if paramsLength > 3 else tableName
            if hiveTableName == "":
                hiveTableName = tableName
            hiveDestDBLevel=tableParams[4].strip().lower() if paramsLength > 4 else ""
            if hiveDestDBLevel ==  "" or hiveDestDBLevel== "pub" :
                hiveDestDBLevel = " "
            else:
                hiveDestDBLevel = "_"+hiveDestDBLevel
            print "generate.py             -> hiveDestDBLevelToken "+ hiveDestDBLevel

            seperator=""
            #variables with Token suffix will be referred in the template files for (job or hive scripts)
            columnsWithoutPartitionToken=""
            hiveColumnsWithoutPartitionToken=""
            createColumnsToken=""
            createPartitionColumn=""
            partitionToken=""
            partitionColumnToken=""
            partitionColumnSelectToken=""
            partitionedByToken=""
            colIndex=1
            tableFound="false"
            partitionrDataType=""
            mapColumnJava=""
            mapColumnSeperator=""
            
            print "generate.py             -> Get metadat Path" + str(getMetadataPath())
            #Open the metadata file
            for line in open( getMetadataPath() ):
               
               if line.strip().lower().startswith( tableName.strip().lower()+"," ):
                   tableFound="true"
                   tableColRow=line.split(",")
                   columnName=tableColRow[1]; columnLength=tableColRow[2] ; columnType=tableColRow[3]
                   decimalTotalDigits=tableColRow[4]; decimalFractionDigits=tableColRow[5]
                   columnId = tableColRow[6]
                   dataType = datatypes.getColumnType(datasource,columnType,columnLength,decimalTotalDigits,decimalFractionDigits)
                   #print columnName ,columnType, columnLength, " ==> dataType = ", dataType
                   columnName=columnName.lower()
                   mapColumnType=datatypes.getMapColumnType(datasource,columnType)
                   if not mapColumnType=="":
                      mapCol = columnName.replace("#","_").lower()
                      if mapCol[0] in "0123456789":
                         mapCol = "_" + mapCol
                      #For now it is hard coded to look for only master_loan_lease_hist table.
                      #In future when we identify timestamp column with 00:00:00 as time, we can generalize
                      if columnName.lower() == "file_date" and tableName.strip().lower() == "master_loan_lease_hist":
                         mapColumnType = "java.sql.Date"
                      mapColumnJava = mapColumnJava + mapColumnSeperator + mapCol + "="+mapColumnType
                      mapColumnSeperator=","

                   if partitionCol.lower() == columnName:
                       partitionToken = "partition ( " + partitionCol + " ) "
                       partitionColumnSelectToken = "," + columnName
                       partitionColumnToken = columnName
                       partitionedByToken = "partitioned by ( " + partitionCol + " " + dataType + " )"
                       partitionDataType=dataType
                       createPartitionColumn = partitionCol + " " + dataType
                   else:
                       if colIndex%5 == 0:
                          columnsWithoutPartitionToken = columnsWithoutPartitionToken + "\\\n" + seperator + columnName.lower()
                          hiveColumnsWithoutPartitionToken = hiveColumnsWithoutPartitionToken + "\\\n" + seperator + columnName.replace("#","_").lower()
                          colIndex=1
                       else:
                          columnsWithoutPartitionToken = columnsWithoutPartitionToken + seperator + columnName.lower()
                          hiveColumnsWithoutPartitionToken = hiveColumnsWithoutPartitionToken + seperator + columnName.replace("#","_").lower()
                          colIndex = colIndex + 1

                       createColumnsToken= createColumnsToken + seperator + "\n" + columnName.replace("#","_").lower() + " " + dataType

                       seperator=","
               else:
                   if tableFound == "true":
                       break 

            if tableFound == "true":
                createStgColumnToken=""
                if not partitionColumnToken == "":
                      createStgColumnToken = createColumnsToken + seperator + createPartitionColumn
                else:
                      createStgColumnToken = createColumnsToken
                    
                
                tableJobProperties = jobTemplateText
                #if not dbUserName == "":
                if not key_store == "service":
                    tableJobProperties = tableJobProperties.replace("${datasourceToken_username_jdbcrefToken}",dbUserName.upper())
                   
                if key_store == "service":
                      tableJobProperties = tableJobProperties.replace("passwordAliasToken","${username}")
                else:
                      tableJobProperties = tableJobProperties.replace("passwordAliasToken",\
                                              jdbcReference.lower() + "_" \
                                              +dbUserName.lower()) 

                #Populate values in jobs.properties.template 
                tableJobProperties = tableJobProperties.replace("datasourceToken",datasource)
                tableJobProperties = tableJobProperties.replace("sourceSystemToken",sourceSystem)
                tableJobProperties = tableJobProperties.replace("keystoreToken",key_store)
                tableJobProperties = tableJobProperties.replace("jdbcrefToken",jdbcReference.lower())
                if database == "":
                   tableJobProperties = tableJobProperties.replace("schemaToken"," ")
                else: 
                   tableJobProperties = tableJobProperties.replace("schemaToken",database.lower() + "." )
                tableJobProperties = tableJobProperties.replace("${TableName}",tableName)
                   
                tableJobProperties = tableJobProperties.replace("${hiveTableNameToken}",hiveTableName)
                tableJobProperties = tableJobProperties.replace("${hiveDestDBLevelToken}",hiveDestDBLevel)
                tableJobProperties = tableJobProperties.replace("${columnsWithoutPartitionToken}", columnsWithoutPartitionToken)
                tableJobProperties = tableJobProperties.replace("${hiveColumnsWithoutPartitionToken}", hiveColumnsWithoutPartitionToken)
                tableJobProperties = tableJobProperties.replace("${partitionColumnToken}",partitionColumnToken)
                tableJobProperties = tableJobProperties.replace("${partitionColumnSelectToken}",partitionColumnSelectToken)
                if partitionColumnSelectToken == "":
                     tableJobProperties = tableJobProperties.replace("${partition_column_select}",partitionColumnSelectToken)
                    
                tableJobProperties = tableJobProperties.replace("${partitionToken}",partitionToken)
                tableJobProperties = tableJobProperties.replace("${partitionedByToken}",partitionedByToken)
                tableJobProperties = tableJobProperties.replace("${numMappers}",numMappers)
                tableJobProperties = tableJobProperties.replace("appNameToken",appName)
                tableJobProperties = tableJobProperties.replace("subappToken",sub_app)
                
                
                if int(numMappers.strip()) > 1:
                	 if datasource.lower() == "sqlserver":
                	    tableJobProperties = tableJobProperties.replace("${splitBy}",splitBy+ " % ${num_mappers}")
                	 else:
                	 	  tableJobProperties = tableJobProperties.replace("${splitBy}",splitBy+ " MOD ${num_mappers}")
                else: 
                	 tableJobProperties = tableJobProperties.replace("${splitBy}",splitBy)
                
                tableJobProperties = tableJobProperties.replace("${mapColumnJavaToken}",mapColumnJava)
                
                
                print "generate.py             -> writing to " + wfPath+"/"+tableName + ".properties"
                outfile = open(wfPath+"/"+tableName + ".properties","w")
                outfile.write(tableJobProperties)
                outfile.close()
                subprocess.call(["chmod", "777",wfPath+"/" + tableName +".properties"])

                print "generate.py             -> writing to " + app_src_path+"/hive/"+tableName + "_create_stg.hql"
                #Populate staging table create script
                outfile = open(app_src_path + "/hive/" + tableName+ "_create_stg.hql","w")
                createStgText = createStgTemplateText
                createStgText = createStgText.replace( "${hiveconf:stage_table}",tableName)
                createStgText = createStgText.replace( "${hiveconf:createStgColumns}",createStgColumnToken) 
                outfile.write( createStgText)
                outfile.close()
                subprocess.call(["chmod", "777",app_src_path+"/hive/" + tableName +"_create_stg.hql"])

                #populate final/parquet table create script
                outfile = open(app_src_path + "/hive/" + tableName + "_create_parquet.hql","w")
                createParquetText = createParquetTemplateText
                createParquetText = createParquetText.replace("${hiveconf:table}",hiveTableName)
                createParquetText=createParquetText.replace("${hiveconf:columnsWithoutPartition}",createColumnsToken) 
                createParquetText=createParquetText.replace("${hiveconf:partitionedBy}",partitionedByToken)
                outfile.write(createParquetText)
                outfile.close()

                subprocess.call(["chmod", "777",app_src_path+"/hive/" + tableName +"_create_parquet.hql"])

                tablesGenerated=tablesGenerated + tableName + "\n"
            
            continue

        outFilePath = app_wrk_path + "/"+sqoopparams +".list"
        print "generate.py             -> tablesGeneratedList is in location  "+ outFilePath
        if not tablesGenerated == "":
            outfile = open(outFilePath,"w")
            outfile.write(tablesGenerated) 
            outfile.close()
           
        #    for line in open(outFilePath):
        #        tableName = line.strip()
        #        filePathToCopy = jobOutputDir+"/" + tableName + "/" + tableName +".properties"
        #        #print "generate.py             -> calling " + "cp " +  jobOutputDir+"/" + tableName + "/" + tableName +".properties " +wfPath
        #        status = subprocess.call(["cp", filePathToCopy, wfPath])
        #        if status != 0:
        #           print "generate.py             -> Failing copying file ", status
        #           sys.exit(1)
        #        subprocess.call(["chmod", "777",filePathToCopy])
        #        subprocess.call(["chmod", "777",wfPath+"/"+tableName+".properties"])
                       

def getMetadataPath():
    return options.metadatapath

def getWorkflowPath():
    return options.workflowpath
 
if __name__ == "__main__":

     #Validate the input parameters
     parser = optparse.OptionParser()
     options = validateOptions()
     
     common_root_path= "data/bdp"+options.env + "/bdh/"+options.env_ver+"/global"

     sys.path.append( os.path.expanduser(common_root_path + "/code/scripts/") )
     import envvars
     envvars.populate(options.env,options.env_ver,options.app,options.sub_app)

     sqoopParamFilePath = envvars.list['lfs_app_config'] + "/"+options.sqoopparams
     tableName = options.tableName
     env_ver = options.env_ver
     print "generate.py             -> sqoopParamFilePath = " + sqoopParamFilePath

     #Read the input properties file (i.e) file with app, database, user id and table informations
     config = ConfigParser.ConfigParser()
     config.readfp(open(sqoopParamFilePath))
     appName = config.get("DataSourceInfo","app")
     try:
       sub_app = config.get("DataSourceInfo","sub_app")
     except ConfigParser.NoOptionError:
       sub_app="data_ingest"

     datasource = config.get("DataSourceInfo","datasource")
     database=""
     if datasource.lower() == "oracle":
        database = config.get("InfoForMetadata","database").upper()
     sourceSystem = config.get("DataSourceInfo","sourcesystem")
     
     jdbcReference = config.get("DataSourceInfo","jdbc_ref")
     dbUserName= config.get("DataSourceInfo","db_user_id")
     print "generate.py             -> jdbcReference = "+ jdbcReference

     if not tableName:
        tableItems = config.items("TableInfo")
     else:
        tableItems = [ (tableName, config.get("TableInfo",tableName) ) ]
     if len(tableItems) <= 0:
         print "generate.py             -> Table configuration with 'TableInfo' tag could not be found in ", sqoopParamFilePath
         sys.exit(1)

     generateScripts( envvars.list['lfs_global_scripts'],envvars.list['lfs_app_src'],envvars.list['lfs_app_config'],datasource,sourceSystem, jdbcReference,dbUserName, \
                                env_ver, appName,sub_app, tableItems, sqoopParamFilePath, options.sqoopparams, options.key_store,database)

