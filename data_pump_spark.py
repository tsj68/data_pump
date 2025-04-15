from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import logging
from typing import Dict, Optional

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataConnector:
    """PySpark数据库连接器"""
    
    JDBC_MAP = {
        'mysql': 'jdbc:mysql',
        'oracle': 'jdbc:oracle:thin',
        'postgresql': 'jdbc:postgresql',
        'mssql': 'jdbc:sqlserver'
    }
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def get_jdbc_url(self, db_config: Dict) -> str:
        """生成JDBC连接URL"""
        db_type = db_config['type'].lower()
        if db_type not in self.JDBC_MAP:
            raise ValueError(f"不支持的数据库类型: {db_type}")
        
        if db_type == 'oracle':
            return f"jdbc:oracle:thin:@{db_config['host']}:{db_config.get('port', 1521)}:{db_config['database']}"
        else:
            return f"{self.JDBC_MAP[db_type]}://{db_config['host']}:{db_config.get('port', '')}/{db_config['database']}"

class SparkDataMigrator:
    """基于PySpark的数据迁移器"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataMigration") \
            .getOrCreate()
        self.connector = SparkDataConnector(self.spark)
    
    def read_data(self, source_config: Dict, query: str, schema: Optional[StructType] = None):
        """从源数据库读取数据"""
        jdbc_url = self.connector.get_jdbc_url(source_config)
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"({query}) as tmp") \
                .option("user", source_config['user']) \
                .option("password", source_config['password']) \
                .option("fetchsize", "10000") \
                .schema(schema) \
                .load()
            logger.info(f"成功读取数据，记录数: {df.count()}")
            return df
        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            raise
    
    def write_data(self, target_config: Dict, df, table_name: str, mode: str = "append"):
        """写入数据到目标数据库"""
        jdbc_url = self.connector.get_jdbc_url(target_config)
        record_count = df.count()
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", target_config['user']) \
                .option("password", target_config['password']) \
                .option("batchsize", "10000") \
                .mode(mode) \
                .save()
            logger.info(f"成功写入数据到表 {table_name}，记录数: {record_count}")
        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            raise
    
    def migrate(self, source_config: Dict, target_config: Dict, query: str, target_table: str):
        """完整迁移流程"""
        try:
            logger.info("开始从源数据库读取数据...")
            df = self.read_data(source_config, query)
            total_records = df.count()
            
            logger.info(f"开始写入目标数据库，总记录数: {total_records}...")
            self.write_data(target_config, df, target_table)
            
            logger.info(f"数据迁移完成! 共迁移 {total_records} 条记录")
        except Exception as e:
            logger.error(f"迁移过程出错: {str(e)}")
            raise
        finally:
            self.spark.stop()

# 使用示例
if __name__ == "__main__":
    # 源数据库配置
    source_config = {
        'type': 'mysql',
        'host': 'source_host',
        'port': 3306,
        'user': 'username',
        'password': 'password',
        'database': 'source_db'
    }
    
    # 目标数据库配置
    target_config = {
        'type': 'postgresql',
        'host': 'target_host',
        'port': 5432,
        'user': 'username', 
        'password': 'password',
        'database': 'target_db'
    }
    
    # 创建迁移器并执行
    migrator = SparkDataMigrator()
    migrator.migrate(
        source_config=source_config,
        target_config=target_config,
        query="SELECT * FROM source_table WHERE create_date > '2023-01-01'",
        target_table="target_table"
    )
