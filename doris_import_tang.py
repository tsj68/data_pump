
from pyspark.sql import SparkSession
import pymysql
import sys
import api_conn

if __name__=='__main__':
    argv=sys.argv
 
    if '-source' not in argv or '-target' not in argv or '-target_db' not in argv:
        print('error','pls input -source and -target or -target_db')
        sys.exit()
    try:        
        source=argv[argv.index('-source')+1]
        target=argv[argv.index('-target')+1]
        target_db=argv[argv.index('-target_db')+1]
        conn_info=api_conn.get_conn_info(target_db)
        
        if '-partition' in argv:
            is_partition=True
            partition=argv[argv.index('-partition')+1]    
        else:
            is_partition=False 
    except Exception as e:
        print('error',str(e.args))
        sys.exit()
    
    try:
        spark=SparkSession.builder.appName('DorisImport-' + target).enableHiveSupport().getOrCreate()
        conn=pymysql.connect(host=conn_info['conn_server'],port=int(conn_info['conn_port']),user=conn_info['conn_username'],password=conn_info['conn_password'])
        cursor=conn.cursor()
        if is_partition:
            delete_sql="""truncate table {target} partition p{partition}
                          """.format(target=target,partition=partition)
        else:
            delete_sql="""truncate table {target}
                          """.format(target=target)
        cursor.execute(delete_sql)
        conn.commit()
        df=spark.sql(source)
        df.write.format("doris") \
          .option("doris.table.identifier", target) \
          .option("doris.fenodes", conn_info['conn_server']+':'+ conn_info['conn_httpport']) \
          .option("user", conn_info['conn_username']) \
          .option("password", conn_info['conn_password']) \
          .save()
        if is_partition:
            result_sql="""select count(*) from {target} partition p{partition}""".format(target=target,partition=partition)
        else:
            result_sql="""select count(*) from {target}""".format(target=target)
        cursor.execute(result_sql)  
        result=cursor.fetchall()[0][0]
        print('sucess',result)
    except Exception as e:
        print('error',str(e.args))
        sys.exit()
