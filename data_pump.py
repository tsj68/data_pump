import pandas as pd
from sqlalchemy import create_engine, exc
from contextlib import contextmanager
import logging
from typing import Dict, List, Optional, Union

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    """支持多种数据库类型的连接器"""
    
    # 数据库驱动配置映射
    DRIVER_MAP = {
        'mysql': 'mysql+pymysql',
        'oracle': 'oracle+cx_oracle',
        'postgresql': 'postgresql+psycopg2',
        'sqlite': 'sqlite',
        'hive': 'hive',
        'mssql': 'mssql+pyodbc'
    }
    
    @staticmethod
    def get_connection_string(db_config: Dict) -> str:
        """生成SQLAlchemy连接字符串"""
        db_type = db_config['type'].lower()
        driver = DatabaseConnector.DRIVER_MAP.get(db_type, db_type)
        
        if db_type == 'sqlite':
            return f"sqlite:///{db_config['database']}"
        elif db_type == 'hive':
            return f"hive://{db_config.get('host', 'localhost')}:{db_config.get('port', 10000)}/{db_config['database']}"
        else:
            return (
                f"{driver}://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config.get('port', '')}/"
                f"{db_config['database']}"
            )

    @contextmanager
    def connect(self, db_config: Dict):
        """上下文管理器处理数据库连接"""
        conn_str = self.get_connection_string(db_config)
        engine = None
        try:
            engine = create_engine(conn_str)
            conn = engine.connect()
            yield conn
        except exc.SQLAlchemyError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise
        finally:
            if engine:
                engine.dispose()

class DataMigrator:
    def __init__(self):
        self.connector = DatabaseConnector()
    
    def read_data(self, source_config: Dict, query: str, chunksize: Optional[int] = None) -> Union[List[Dict], pd.DataFrame]:
        """从源数据库读取数据"""
        try:
            with self.connector.connect(source_config) as conn:
                if chunksize:
                    # 分批读取大数据
                    return pd.read_sql(query, conn, chunksize=chunksize)
                else:
                    # 小数据直接读取
                    return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"数据读取失败: {str(e)}")
            raise
    
    def write_data(self, target_config: Dict, data: Union[List[Dict], pd.DataFrame], table_name: str, if_exists: str = 'append'):
        """写入数据到目标数据库"""
        try:
            with self.connector.connect(target_config) as conn:
                if isinstance(data, pd.DataFrame):
                    data.to_sql(
                        table_name, 
                        conn, 
                        if_exists=if_exists, 
                        index=False
                    )
                else:
                    # 列表字典数据转换为DataFrame
                    df = pd.DataFrame(data)
                    df.to_sql(
                        table_name, 
                        conn, 
                        if_exists=if_exists, 
                        index=False
                    )
                logger.info(f"成功写入数据到表 {table_name}")
        except Exception as e:
            logger.error(f"数据写入失败: {str(e)}")
            raise
    
    def migrate(self, source_config: Dict, target_config: Dict, query: str, target_table: str, chunksize: Optional[int] = None):
        """完整迁移流程"""
        try:
            # 1. 读取数据
            logger.info("开始从源数据库读取数据...")
            data = self.read_data(source_config, query, chunksize)
            
            # 2. 写入数据
            logger.info("开始写入目标数据库...")
            if chunksize and isinstance(data, pd.io.sql.DatabaseIterator):
                for chunk in data:
                    self.write_data(target_config, chunk, target_table)
            else:
                self.write_data(target_config, data, target_table)
                
            logger.info("数据迁移完成!")
        except Exception as e:
            logger.error(f"迁移过程出错: {str(e)}")
            raise

# 使用示例
if __name__ == "__main__":
    # 源数据库配置 (可以是MySQL/Oracle/Hive等)
    source_config = {
        'type': 'mysql',  # 支持mysql/oracle/hive/postgresql/sqlite等
        'host': 'source_host',
        'port': 3306,
        'user': 'username',
        'password': 'password',
        'database': 'source_db'
    }
    
    # 目标数据库配置
    target_config = {
        'type': 'oracle',  # 可以是不同类型的数据库
        'host': 'target_host',
        'port': 1521,
        'user': 'username',
        'password': 'password',
        'database': 'target_db'
    }
    
    # SQL查询语句
    query = "SELECT * FROM source_table WHERE create_date > '2023-01-01'"
    
    # 创建迁移器并执行
    migrator = DataMigrator()
    
    # 小数据量直接迁移
    migrator.migrate(
        source_config=source_config,
        target_config=target_config,
        query=query,
        target_table="target_table"
    )
    
    # 大数据量分批迁移 (示例)
    # migrator.migrate(
    #     source_config=source_config,
    #     target_config=target_config,
    #     query=query,
    #     target_table="target_table",
    #     chunksize=10000
    # )