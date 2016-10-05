import sys

def getColumnType(datasource,columnType,maxLen,totalDigits,totalFraction):
    columnType = columnType.lower().strip()
    if (datasource.lower() == "teradata"):
         return mapTeradataType(columnType,maxLen,totalDigits,totalFraction)
    elif (datasource.lower() == 'sqlserver'):
         return mapSqlServerType(columnType,maxLen,totalDigits,totalFraction)
    elif datasource.lower() == 'oracle':
         return mapOracleType(columnType,maxLen, totalDigits, totalFraction)
           
def getMapColumnType(datasource,columnType):
    columnType = columnType.lower().strip()
    if (datasource.lower() == "oracle"):
        if columnType == "binary_double":
            return "String"

    return "String"
   

def mapTeradataType(columnType,maxLen,totalDigits,totalFraction):
    if (totalDigits == "null" or totalDigits==""):
         numberTypeTotalDigits = "0"
    else:
         numberTypeTotalDigits = totalDigits
        
    return {
         'bv' : 'binary',
         'cf' : 'char' if int(maxLen) <=0 else 'char (' + maxLen + ")",
         'cv' : 'varchar' if int(maxLen) <= 0 else 'varchar (' + maxLen + ")",
    #     'd'  : "decimal(" + totalDigits +"," + totalFraction +")",
         'd'  :  defineDecimalType(columnType,totalDigits ,totalFraction ),
         'da' : 'string',
         'i'  : 'int',
         'i1' : 'tinyint',
         'i2' : 'smallint',
         'i8' : 'bigint',
         'n'  : 'int' if abs(int(numberTypeTotalDigits)) > 35 else 'decimal(' + totalDigits +"," + totalFraction +")" ,
         'ts' : 'timestamp'
    }.get(columnType,"string")

def defineDecimalType(columnType, totalDigits, totalFraction):
    if (totalDigits == "null" or totalDigits==""):
         return ""
    if (totalDigits == totalFraction):
           totalDigits = str(int(totalDigits)  + 1)
    
    return "decimal(" + totalDigits +"," + totalFraction +")"

def mapSqlServerType(columnType,maxLen,totalDigits,totalFraction):
   return{
         'bit'          :'Boolean' ,
         'char'         :'char' if int(maxLen) <=0 else 'char (' + maxLen + ")",
         'decimal'      :"decimal(" + totalDigits +"," + totalFraction +")",
         'numeric'      :"decimal(" + totalDigits +"," + totalFraction +")",
         'money'        :"decimal(" + totalDigits +"," + totalFraction +")",
         'float'        :'double' ,
         'int'          :'int' ,
         'smallint'     :'smallint' ,
         'date'         :'string' ,
         'datetime'     :'string' ,
         'smalldatetime':'string' ,
         'tinyint'      :'tinyint',
         'nvarchar'     :'string' if int(maxLen) <= 0 else 'varchar (' + maxLen + ")",
         'varchar'      :'string' if int(maxLen) <= 0 else 'varchar (' + maxLen + ")"
   }.get(columnType,'String')

def mapOracleType(columnType,maxLen,totalDigits,totalFraction):
   return{
       'binary_double' : 'double',
       'date'          : 'string',
       'number'        : "decimal(" + totalDigits +"," + totalFraction +")",
       'varchar2'      : 'string'
   }.get(columnType.lower(),'String')

