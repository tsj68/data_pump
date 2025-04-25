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
            return f"hive://hadoop@{db_config.get('host', 'localhost')}:{db_config.get('port', 10000)}/{db_config['database']}"
        elif db_type == 'oracle':
            # Oracle 使用服务名（service_name）的格式
            return (
                f"oracle+cx_oracle://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config.get('port', 1521)}/"
                f"?service_name={db_config['database']}"
            )
        else:
            return (
                f"{driver}://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config.get('port', '')}/"
                f"{db_config['database']}"
            )

    def get_engine(self, db_config: Dict):
        """生成并返回 SQLAlchemy 引擎"""
        conn_str = self.get_connection_string(db_config)
        return create_engine(conn_str)

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

    # 修改后的 read_data 方法（方法1示例）
    def read_data(self, source_config: Dict, query: str, chunksize: Optional[int] = None) -> pd.DataFrame:
        try:
            with self.connector.connect(source_config) as conn:
                # 获取底层 pyhive 连接
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                cursor.execute(query)
                data = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df = pd.DataFrame(data, columns=columns)
                return df
        except Exception as e:
            logger.error(f"数据读取失败: {str(e)}")
            raise

    def write_data(self, target_config: Dict, data: Union[List[Dict], pd.DataFrame], table_name: str,
                   if_exists: str = 'append'):
        """写入数据到目标数据库（修复版）"""
        try:
            engine = self.connector.get_engine(target_config)
            # 通过 engine 创建连接并管理事务
            with engine.begin() as conn:
                if isinstance(data, pd.DataFrame):
                    data.to_sql(
                        table_name,
                        conn,  # 传递连接对象而非引擎
                        if_exists=if_exists,
                        index=False
                    )
                else:
                    df = pd.DataFrame(data)
                    df.to_sql(
                        table_name,
                        conn,  # 传递连接对象而非引擎
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
        'type': 'hive',  # 支持mysql/oracle/hive/postgresql/sqlite等
        'host': '10.203.18.65',
        'port': 7001,
        'user': 'hadoop',
        'password': 'xxx',
        'database': 'data_cs'
    }
    
    # 目标数据库配置
    target_config = {
        'type': 'oracle',  # 可以是不同类型的数据库
        'host': '192.168.5.194',
        'port': 1521,
        'user': 'zfl',
        'password': 'ncbz$741',
        'database': 'sale'
    }
    
    # SQL查询语句
    query = "SELECT cdate,store_id,item_id,sum(hs_wq_amt) hs_wq_amt FROM data_cs.dwd_trd_salord_item_di where cdate=date '2025-04-20' group by cdate,store_id,item_id"
    
    # 创建迁移器并执行
    migrator = DataMigrator()
    
    # 小数据量直接迁移
    migrator.migrate(
        source_config=source_config,
        target_config=target_config,
        query=query,
        target_table="TEST"
    )
    
    # 大数据量分批迁移 (示例)
    # migrator.migrate(
    #     source_config=source_config,
    #     target_config=target_config,
    #     query=query,
    #     target_table="target_table",
    #     chunksize=10000
    # )