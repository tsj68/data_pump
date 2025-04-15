# 数据迁移工具需求设计文档

## 1. 需求分析

### 1.1 功能需求
- 支持多种数据库类型：MySQL、Oracle、PostgreSQL、SQLite、Hive、MS SQL
- 实现数据库连接管理，包括连接字符串生成和连接池管理
- 提供数据读取功能，支持全量读取和分批读取
- 提供数据写入功能，支持DataFrame和字典列表格式
- 完整的迁移流程封装，包含读取-传输-写入全流程
- 完善的日志记录和错误处理机制

### 1.2 非功能需求
- 性能：支持大数据量分批处理，避免内存溢出
- 可靠性：保证数据传输的完整性和一致性
- 可扩展性：易于添加新的数据库类型支持
- 可维护性：良好的代码结构和文档支持

## 2. 系统设计

### 2.1 架构设计
```
┌───────────────────────┐
│      DataMigrator     │
├───────────────────────┤
│ + read_data()         │
│ + write_data()        │
│ + migrate()           │
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│   DatabaseConnector   │
├───────────────────────┤
│ + get_connection_str()│
│ + connect()           │
└───────────────────────┘
```

### 2.2 类设计

#### DatabaseConnector类
- 职责：管理数据库连接
- 核心方法：
  - `get_connection_string()`: 生成数据库连接字符串
  - `connect()`: 创建数据库连接(上下文管理器)

#### DataMigrator类
- 职责：数据迁移核心逻辑
- 核心方法：
  - `read_data()`: 从源数据库读取数据
  - `write_data()`: 向目标数据库写入数据  
  - `migrate()`: 完整迁移流程封装

### 2.3 关键流程

#### 数据迁移流程
1. 初始化源和目标数据库配置
2. 创建DataMigrator实例
3. 调用migrate方法执行迁移
4. 读取阶段：分批从源数据库获取数据
5. 写入阶段：分批向目标数据库写入数据
6. 日志记录全过程状态和异常

## 3. 接口说明

### 3.1 数据库配置格式
```python
{
    'type': 'mysql',  # 数据库类型
    'host': 'hostname',
    'port': 3306, 
    'user': 'username',
    'password': 'password',
    'database': 'dbname'
}
```

### 3.2 迁移方法参数
```python
migrate(
    source_config: Dict,  # 源数据库配置
    target_config: Dict,  # 目标数据库配置 
    query: str,           # 查询语句
    target_table: str,    # 目标表名
    chunksize: int = None # 分批大小
)
```

## 4. 使用示例

见data_pump.py文件末尾的__main__部分
