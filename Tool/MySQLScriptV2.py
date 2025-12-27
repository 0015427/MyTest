import pymysql
from typing import List, Tuple, Any, Optional
import logging
import time
from tqdm import tqdm
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
from datetime import datetime


class MySQLBatchProcessor:
    """
    MySQL批量数据处理工具类
    支持批量插入、更新、删除等操作
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,
                 charset: str = 'utf8mb4', auto_optimize: bool = False):
        """
        初始化数据库连接参数

        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 用户名
            password: 密码
            database: 数据库名
            charset: 字符集
            auto_optimize: 是否自动应用批量操作优化设置
        """
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset
        }
        self.auto_optimize = auto_optimize
        self.connection = None

    def connect(self) -> bool:
        """
        建立数据库连接

        Returns:
            bool: 连接是否成功
        """
        try:
            self.connection = pymysql.connect(**self.config)
            if self.auto_optimize:
                self._apply_bulk_optimizations()
            return True
        except Exception as e:
            logging.error(f"数据库连接失败: {e}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            if self.auto_optimize:
                self._restore_bulk_optimizations()
            self.connection.close()
            self.connection = None

    def batch_execute(self, sql: str, data_list: List[Tuple[Any]],
                      batch_size: int = 1000, show_progress: bool = False,
                      use_multithreading: bool = False, max_workers: int = 4) -> bool:
        """
        批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表，每个元素是一个元组
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 执行是否成功
        """
        if use_multithreading:
            return self._batch_execute_multithreaded(sql, data_list, batch_size, show_progress, max_workers)
        else:
            return self._batch_execute_single_threaded(sql, data_list, batch_size, show_progress)

    def _batch_execute_single_threaded(self, sql: str, data_list: List[Tuple[Any]],
                                       batch_size: int, show_progress: bool) -> bool:
        """
        单线程批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条

        Returns:
            bool: 执行是否成功
        """
        if not self.connection:
            if not self.connect():
                return False

        cursor = self.connection.cursor()
        progress_bar = None
        is_success = True

        try:
            # 分批处理数据
            total_batches = (len(data_list) + batch_size - 1) // batch_size
            progress_bar = tqdm(total=len(data_list), desc="处理进度", disable=not show_progress,
                                ncols=100, leave=False)

            for i in range(0, len(data_list), batch_size):
                batch_data = data_list[i:i + batch_size]
                cursor.executemany(sql, batch_data)
                self.connection.commit()
                progress_bar.update(len(batch_data))
                logging.debug(f"已处理 {min(i + batch_size, len(data_list))}/{len(data_list)} 条记录")

            progress_bar.close()

        except Exception as e:
            logging.error(f"批量执行失败: {e}")
            self.connection.rollback()
            is_success = False
        finally:
            if 'progress_bar' in locals():
                progress_bar.close()
            cursor.close()

        return is_success

    def _batch_execute_multithreaded(self, sql: str, data_list: List[Tuple[Any]],
                                     batch_size: int, show_progress: bool, max_workers: int) -> bool:
        """
        多线程批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条
            max_workers: 最大线程数

        Returns:
            bool: 执行是否成功
        """
        # 将数据分割成多个批次
        batches = [data_list[i:i + batch_size] for i in range(0, len(data_list), batch_size)]

        progress_bar = tqdm(total=len(data_list), desc="多线程处理进度", disable=not show_progress,
                            ncols=100, leave=False)

        is_success = True
        lock = threading.Lock()

        def process_batch(batch_data):
            """处理单个批次的数据"""
            local_connection = None
            local_cursor = None
            try:
                # 每个线程创建独立的数据库连接
                local_connection = pymysql.connect(**self.config)
                # 多线程也应用批量操作优化
                if self.auto_optimize:
                    self._apply_bulk_optimizations(local_connection)

                local_cursor = local_connection.cursor()
                local_cursor.executemany(sql, batch_data)
                local_connection.commit()

                with lock:
                    progress_bar.update(len(batch_data))

                return True
            except Exception as e:
                logging.error(f"批次处理失败: {e}")
                if local_connection:
                    local_connection.rollback()
                return False
            finally:
                if local_cursor:
                    local_cursor.close()
                if local_connection:
                    self._restore_bulk_optimizations(local_connection)
                    local_connection.close()

        try:
            # 使用线程池执行任务
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}

                # 等待所有任务完成
                for future in as_completed(future_to_batch):
                    if not future.result():
                        is_success = False

        except Exception as e:
            logging.error(f"多线程执行失败: {e}")
            is_success = False
        finally:
            progress_bar.close()

        return is_success

    def batch_insert(self, table_name: str, columns: List[str],
                     data_list: List[Tuple[Any]], batch_size: int = 1000,
                     show_progress: bool = False, use_multithreading: bool = False,
                     max_workers: int = 4) -> bool:
        """
        批量插入数据

        Args:
            table_name: 表名
            columns: 列名列表
            data_list: 数据列表
            batch_size: 批量大小
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 插入是否成功
        """
        # 构造INSERT语句
        columns_str = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        return self.batch_execute(sql, data_list, batch_size, show_progress,
                                  use_multithreading, max_workers)

    def batch_update(self, table_name: str, set_columns: List[str],
                     where_column: str, data_list: List[Tuple[Any]],
                     batch_size: int = 1000, show_progress: bool = False,
                     use_multithreading: bool = False, max_workers: int = 4) -> bool:
        """
        批量更新数据

        Args:
            table_name: 表名
            set_columns: 需要更新的列名列表
            where_column: WHERE条件列名
            data_list: 数据列表，最后一个元素是WHERE条件值
            batch_size: 批量大小
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 更新是否成功
        """
        # 构造UPDATE语句
        set_clause = ', '.join([f"`{col}` = %s" for col in set_columns])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `{where_column}` = %s"

        return self.batch_execute(sql, data_list, batch_size, show_progress,
                                  use_multithreading, max_workers)

    def execute_query(self, sql: str, params: Optional[Tuple[Any]] = None) -> List[Tuple[Any]]:
        """
        执行查询语句

        Args:
            sql: 查询SQL语句
            params: 参数元组

        Returns:
            List[Tuple[Any]]: 查询结果
        """
        if not self.connection:
            if not self.connect():
                return []

        cursor = self.connection.cursor()
        result = []

        try:
            cursor.execute(sql, params or ())
            result = cursor.fetchall()
        except Exception as e:
            logging.error(f"查询执行失败: {e}")
        finally:
            cursor.close()

        return result

    def load_data_from_file(self, table_name: str, csv_file_path: str, use_local: bool = False) -> bool:
        """
        使用LOAD DATA INFILE快速导入数据

        Args:
            table_name: 目标表名
            csv_file_path: CSV文件路径
            use_local: 是否使用LOCAL INFILE（需要客户端文件权限）

        Returns:
            bool: 导入是否成功
        """
        if not self.connection:
            if not self.connect():
                return False

        cursor = self.connection.cursor()
        try:
            # 优化设置
            self._apply_bulk_optimizations()

            # 根据是否使用LOCAL调整SQL语句
            if use_local:
                load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_file_path}'
                INTO TABLE `{table_name}`
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 0 ROWS
                """
            else:
                load_sql = f"""
                LOAD DATA INFILE '{csv_file_path}'
                INTO TABLE `{table_name}`
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 0 ROWS
                """

            cursor.execute(load_sql)
            self.connection.commit()

            logging.info(f"成功导入 {cursor.rowcount} 条记录")
            return True

        except Exception as e:
            logging.error(f"数据导入失败: {e}")
            self.connection.rollback()
            return False
        finally:
            self._restore_bulk_optimizations()
            cursor.close()

    def _apply_bulk_optimizations(self, connection= None):
        """应用批量操作优化设置"""
        conn = connection or self.connection
        if conn:
            cursor = conn.cursor()
            try:
                # 会话级别变量，仅对当前会话有效
                cursor.execute("SET autocommit=0")
                cursor.execute("SET unique_checks=0")
                cursor.execute("SET foreign_key_checks=0")
                if not connection: # 仅在主连接上设置日志
                    logging.info("数据库已为批量插入优化,关闭自动提交模式,关闭唯一性约束检查,关闭外键约束检查")
            except Exception as e:
                logging.error(f"批量操作优化设置失败: {e}")
            finally:
                cursor.close()


    def _restore_bulk_optimizations(self, connection= None):
        """还原优化设置"""
        conn = connection or self.connection
        if conn:
            cursor = conn.cursor()
            try:
                # 会话级别变量，仅对当前会话有效
                cursor.execute("SET autocommit=1")
                cursor.execute("SET unique_checks=1")
                cursor.execute("SET foreign_key_checks=1")
                if not connection: # 仅在主连接上设置日志
                    logging.info("数据库已还原为默认设置")
            except Exception as e:
                logging.error(f"还原优化设置失败: {e}")
            finally:
                cursor.close()

    def batch_insert_with_relationships(self, table_configs: List[dict],
                                        batch_size: int = 1000,
                                        show_progress: bool = False) -> bool:
        """
        批量插入具有关联关系的多表数据

        Args:
            table_configs: 包含表配置的列表，每个配置包含表名、列名、数据和关系信息
            batch_size: 批量大小
            show_progress: 是否显示进度条

        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            if not self.connect():
                return False

        cursor = self.connection.cursor()

        try:
            # 按依赖顺序处理表（先插入被引用的表，后插入引用表）
            ordered_configs = self._order_tables_by_dependency(table_configs)

            for config in ordered_configs:
                table_name = config['table_name']
                columns = config['columns']
                data_list = config['data']

                # 如果表有依赖关系，需要处理外键
                if 'foreign_key_mapping' in config:
                    # 根据父表生成的ID更新当前表的外键
                    data_list = self._update_foreign_keys(
                        data_list, config['foreign_key_mapping']
                    )

                # 批量插入当前表数据
                self.batch_insert(table_name, columns, data_list, batch_size, show_progress)

                # 保存当前表的ID映射（如果需要被其他表引用）
                if 'id_mapping_key' in config:
                    self._save_id_mapping(config['id_mapping_key'], data_list)

            self.connection.commit()
            return True

        except Exception as e:
            logging.error(f"多表批量插入失败: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def _order_tables_by_dependency(self, table_configs: List[dict]) -> List[dict]:
        """根据外键依赖关系对表进行排序"""
        # 创建依赖图
        dependencies = {}
        table_config_map = {config['table_name']: config for config in table_configs}

        for config in table_configs:
            table_name = config['table_name']
            dependencies[table_name] = []

            if 'foreign_key_mapping' in config:
                # 添加对父表的依赖
                for fk_info in config['foreign_key_mapping']:
                    parent_table = fk_info['parent_table']
                    dependencies[table_name].append(parent_table)

        # 拓扑排序
        visited = set()
        order = []

        def dfs(table_name):
            if table_name in visited:
                return
            visited.add(table_name)

            for dep in dependencies.get(table_name, []):
                dfs(dep)

            order.append(table_config_map[table_name])

        for table_name in dependencies:
            dfs(table_name)

        return order

    def _update_foreign_keys(self, data_list: List[Tuple], foreign_key_mappings: List[dict]) -> List[Tuple]:
        """更新数据中的外键值"""
        updated_data = []

        for row in data_list:
            row_list = list(row)
            for mapping in foreign_key_mappings:
                # 根据映射规则更新外键值
                fk_column_index = mapping['column_index']
                parent_mapping_key = mapping['parent_mapping_key']

                # 从已保存的ID映射中获取合适的父ID
                parent_ids = self._get_saved_id_mapping(parent_mapping_key)
                if parent_ids:
                    # 随机选择一个父ID（可根据业务逻辑调整）
                    selected_parent_id = random.choice(parent_ids)
                    row_list[fk_column_index] = selected_parent_id

            updated_data.append(tuple(row_list))

        return updated_data

    def _save_id_mapping(self, mapping_key: str, data_list: List[Tuple]):
        """保存表的ID映射，供其他表引用"""
        if not hasattr(self, '_id_mappings'):
            self._id_mappings = {}

        # 假设ID是第一列（可根据实际情况调整）
        ids = [row[0] for row in data_list]
        self._id_mappings[mapping_key] = ids

    def _get_saved_id_mapping(self, mapping_key: str) -> List:
        """获取已保存的ID映射"""
        if hasattr(self, '_id_mappings') and mapping_key in self._id_mappings:
            return self._id_mappings[mapping_key]
        return []




class MultiTableDataGenerator:
    """多表关联数据生成器"""

    def __init__(self, processor: MySQLBatchProcessor):
        self.processor = processor
        self.id_mappings = {}

    def generate_related_data(self, schema_config: dict, record_counts: dict) -> dict:
        """
        生成具有关联关系的多表数据

        Args:
            schema_config: 数据库表结构配置
            record_counts: 每个表需要生成的记录数量

        Returns:
            dict: 生成的数据，键为表名，值为数据列表
        """
        generated_data = {}

        # 按依赖顺序生成数据
        ordered_tables = self._get_ordered_tables(schema_config)

        for table_name in ordered_tables:
            if table_name in record_counts:
                table_data = self._generate_table_data(
                    table_name,
                    schema_config[table_name],
                    record_counts[table_name],
                    generated_data
                )
                generated_data[table_name] = table_data

        return generated_data

    def _get_ordered_tables(self, schema_config: dict) -> List[str]:
        """获取按依赖关系排序的表名列表"""
        dependencies = {}

        for table_name, table_info in schema_config.items():
            dependencies[table_name] = []
            if 'foreign_keys' in table_info:
                for fk in table_info['foreign_keys']:
                    dependencies[table_name].append(fk['references_table'])

        # 拓扑排序
        visited = set()
        order = []

        def dfs(table_name):
            if table_name in visited:
                return
            visited.add(table_name)

            for dep in dependencies.get(table_name, []):
                dfs(dep)

            order.append(table_name)

        for table_name in dependencies:
            dfs(table_name)

        return order

    def _generate_table_data(self, table_name: str, table_schema: dict,
                             count: int, existing_data: dict) -> List[Tuple]:
        """生成单个表的数据"""
        data = []

        for i in range(count):
            row_data = []

            for column in table_schema['columns']:
                col_name = column['name']

                # 检查是否为外键
                is_foreign_key = False
                if 'foreign_keys' in table_schema:
                    for fk in table_schema['foreign_keys']:
                        if fk['column'] == col_name:
                            # 从父表获取ID
                            parent_table = fk['references_table']
                            if parent_table in existing_data and existing_data[parent_table]:
                                # 随机选择一个父表ID
                                parent_ids = [row[0] for row in existing_data[parent_table]]  # 假设ID是第一列
                                row_data.append(random.choice(parent_ids))
                            else:
                                row_data.append(None)  # 如果父表还没有数据，暂时设为NULL
                            is_foreign_key = True
                            break

                if not is_foreign_key:
                    # 生成普通列数据
                    row_data.append(self._generate_column_value(column))

            data.append(tuple(row_data))

        return data

    def _generate_column_value(self, column_schema: dict) -> Any:
        """根据列类型生成值"""
        col_type = column_schema['type'].upper()

        if 'id' in column_schema['name'].lower() or col_type == 'INT' and 'AUTO_INCREMENT' in column_schema.get('extra', ''):
            return None  # ID字段通常由数据库自动生成
        elif col_type.startswith('VARCHAR') or col_type == 'TEXT':
            length = int(re.search(r'\d+', col_type).group()) if re.search(r'\d+', col_type) else 50
            return ''.join(random.choices(string.ascii_letters + string.digits, k=min(length, 8)))
        elif col_type == 'INT':
            return random.randint(1, 1000)
        elif col_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif col_type == 'BOOLEAN':
            return random.choice([True, False])
        else:
            return f"sample_{column_schema['name']}"

    def insert_related_data(self, data_dict: dict, batch_size: int = 1000) -> bool:
        """插入多表关联数据"""
        try:
            for table_name, data_list in data_dict.items():
                if data_list:
                    # 获取表的列名
                    columns = [col['name'] for col in self.get_table_schema(table_name)['columns']]

                    success = self.processor.batch_insert(
                        table_name=table_name,
                        columns=columns,
                        data_list=data_list,
                        batch_size=batch_size,
                        show_progress=True
                    )

                    if not success:
                        logging.error(f"插入表 {table_name} 数据失败")
                        return False

            logging.info("所有表数据插入成功")
            return True

        except Exception as e:
            logging.error(f"插入多表数据失败: {e}")
            return False

    def get_table_schema(self, table_name: str) -> dict:
        """获取表结构信息（简化版）"""
        # 这里可以根据需要实现获取实际表结构的逻辑
        # 为示例，返回一个基本结构
        return {
            'columns': [
                {'name': 'id', 'type': 'INT', 'extra': 'AUTO_INCREMENT'},
                {'name': 'name', 'type': 'VARCHAR(100)'},
                # ... 其他列
            ],
            'foreign_keys': []
        }




def generate_test_data(count: int) -> List[Tuple[Any]]:
    """
    生成测试数据

    Args:
        count: 数据条数

    Returns:
        List[Tuple[Any]]: 测试数据列表
    """
    data = []
    process_bar = tqdm(total=count, desc="生成数据进度", disable= False,
                                ncols=100, leave=False)
    for i in range(count):
        # 生成随机用户名
        username = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        # 生成随机年龄(18-80)
        age = random.randint(18, 80)
        # 生成随机邮箱
        email = f"{username}@example.com"
        # 生成随机城市
        cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都']
        city = random.choice(cities)
        data.append((username, age, email, city))
        process_bar.update()
    process_bar.close()
    return data


def create_test_table(processor: MySQLBatchProcessor):
    """
    创建测试表

    Args:
        processor: MySQLBatchProcessor实例
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        email VARCHAR(100) NOT NULL,
        city VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    cursor = processor.connection.cursor()
    try:
        cursor.execute(create_table_sql)
        processor.connection.commit()
        logging.info("测试表创建成功")
    except Exception as e:
        logging.error(f"创建测试表失败: {e}")
    finally:
        cursor.close()



def plan_one(processor: MySQLBatchProcessor):
    """
    多线程使用批量插入插入数据
    :param processor:
    :return:
    """
    # plan one
    try:
        # 连接数据库
        if not processor.connect():
            print("数据库连接失败")
            exit(1)

        # 创建测试表
        create_test_table(processor)

        # 生成测试数据
        generate_data_count = 10000000
        print(f"正在生成{generate_data_count / 10000}万条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 批量插入数据并显示进度条
        print(f"开始插入{generate_data_count / 10000}万条数据...")
        start_time = time.time()
        is_success = processor.batch_insert(
            table_name='test_users',
            columns=['username', 'age', 'email', 'city'],
            data_list=test_data,
            batch_size=10000,  # 调整批次大小
            show_progress=True,  # 显示进度条
            use_multithreading=True,  # 启用多线程
            max_workers=8  # 设置最大线程数
        )
        insert_time = time.time() - start_time
        print(f"批量插入结果: {'成功' if is_success else '失败'}")
        print(f"插入耗时: {insert_time:.2f}秒")
        print(f"平均每秒插入: {generate_data_count / insert_time:.0f}条记录")

    except Exception as e:
        logging.error(f"测试过程中出现错误: {e}")
    finally:
        # 关闭连接
        processor.disconnect()
