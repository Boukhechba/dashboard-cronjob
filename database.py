# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 12:38:43 2022

@author: mehdi
"""
import pymysql
import pandas as pd
from datetime import datetime,timedelta

class Database:
    """Wrapper for a PostgreSQL database connection.

    Attributes:
        name: The database name.
        user: The database user.
        password: The database password.
        host: The database host.
        port: The database port.
        connection: The current connection to the database.
        cursor: The current database connection's cursor.
    """

    def __init__(self, name, user, password, host, port):
        """Initializes Database with everything required to execute transactions.

        Args:
            name: The database name.
            user: The database user.
            password: The database password.
            host: The database host.
            port: The database port.
        """
        self.name       =   name
        self.user       =   user
        self.password   =   password
        self.host       =   host
        self.port       =   port
        self.connection =   pymysql.connect(database=name, user=user, password=password, host=host, port=port)
        self.cursor     =   self.connection.cursor()

    def close_connection(self):
        """Closes the current database connection. Call after script database transactions are completed."""
        self.cursor.close()
        self.connection.close()

    def create_summary_table(self,name):
        """Creates a summary table.

        Args:
        """
        statement = 'CREATE TABLE boukhech_sensus.summary_'+name.replace("-","_")+'(deviceid varchar(255),timestamp varchar(20), constraint PRIMARY KEY(deviceid,timestamp));'
        self.cursor.execute(statement)
        self.connection.commit()


    def table_exists(self, name):
        """Checks if the given table exists in the current database.

        Args:
            name: The table name to check.

        Returns:
            True if a table with the given name exists in the current database, else False.
        """
        statement = 'select table_name from information_schema.tables where table_name = \'' + name.replace("-","_") + '\';'
        self.cursor.execute(statement)
        self.connection.commit()
        return self.cursor.fetchone() is not None

    def insert_from_csv(self, grouped,datum,name,summary_columns):
        """Inserts the objects contained in a JSON array as entries in the current database.

        Args:
            json_data: A JSON array to insert.
        """
        if datum not in summary_columns:
                #print("nooo")
                self.cursor.execute('''ALTER TABLE boukhech_sensus.summary_'''+name+''' ADD COLUMN  '''+datum+''' integer DEFAULT 0;''')
                summary_columns.append(datum)
        for by,count in grouped.items():
            self.cursor.execute("INSERT INTO boukhech_sensus.summary_"+name+" (deviceid, timestamp,"+datum+") values ("+"'("+str(by[1])+"): Unknown :"+str(by[2]).split(" ")[0]+"','"+by[0]+"',"+str(count)+")ON DUPLICATE KEY UPDATE "+datum+"="+datum+"+"+str(count)+";");
            
        self.connection.commit()
    
    def insert_script_from_csv(self, grouped,name,summary_columns):
        """Inserts the objects contained in a JSON array as entries in the current database.

        Args:
            json_data: A JSON array to insert.
        """
        for by,count in grouped.items():
            datum="ScriptDatum_"+str(by[3])
            if datum not in summary_columns:
                #print("nooo")
                self.cursor.execute('''ALTER TABLE boukhech_sensus.summary_'''+name+''' ADD COLUMN  '''+datum+''' integer DEFAULT 0;''')
                summary_columns.append(datum)
            self.cursor.execute("INSERT INTO boukhech_sensus.summary_"+name+" (deviceid, timestamp,"+datum+") values ("+"'("+str(by[1])+"): Unknown :"+str(by[2]).split(" ")[0]+"','"+by[0]+"',"+str(count)+")ON DUPLICATE KEY UPDATE "+datum+"="+datum+"+"+str(count)+";");
            
        self.connection.commit()
        return summary_columns
    
    def insert_from_pandas(self,name,df,summary_columns):
        for datum in df.columns:
            if datum not in summary_columns:
                #print("nooo")
                self.cursor.execute('''ALTER TABLE boukhech_sensus.summary_'''+name+''' ADD COLUMN  '''+datum+''' integer DEFAULT 0;''')
                summary_columns.append(datum)
        par= []
        duplicate=[]
        for e in df.columns:
            if e!="deviceid" and e!="timestamp":
                duplicate.append(e+"="+e+"+VALUES("+e+")")
            par.append('%s')
        sql="insert into boukhech_sensus.summary_"+name+"("+df.columns.str.cat(sep=',')+") values("+",".join(par)+") ON DUPLICATE KEY UPDATE "+",".join(duplicate)+";"
        self.cursor.executemany(sql,df.values.tolist())
        self.connection.commit()
        return summary_columns