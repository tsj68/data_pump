
from pyspark.sql import SparkSession
import pymysql
import sys

if __name__=='__main__':
    argv=sys.argv
 
    if '-source' not in argv or '-target' not in argv:
        print('error','请输入-source和-target')
        sys.exit()
    try:
        source=argv[argv.index('-source')+1]
        target=argv[argv.index('-target')+1]
        doris_host=argv[argv.index('-doris_host')+1]
        http_port=int(argv[argv.index('-http_port')+1])
        query_port=int(argv[argv.index('-query_port')+1])
        user=argv[argv.index('-user')+1]
        password=argv[argv.index('-password')+1]     
        if '-partition' in argv:
            is_partition=True
            partition=argv[argv.index('-partition')+1]    
        else:
            is_partition=False 
    except Exception as e:
        print('error',str(e.args))
        sys.exit()
    
    try:
        spark=SparkSession.builder.appName('DorisImport').enableHiveSupport().getOrCreate()
        conn=pymysql.connect(host=doris_host,port=query_port,user=user,password=password)
        cursor=conn.cursor()
        if is_partition:
            delete_sql="""delete from {target} partition p{partition} 
                          where cdate=date'{partition}'
                          """.format(target=target,partition=partition)
        else:
            delete_sql="""truncate table {target}
                          """.format(target=target)
        cursor.execute(delete_sql)
        conn.commit()
        df=spark.sql(source)
        df.write.format("doris") \
          .option("doris.table.identifier", target) \
          .option("doris.fenodes", doris_host+':'+str(http_port)) \
          .option("user", user) \
          .option("password", password) \
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
