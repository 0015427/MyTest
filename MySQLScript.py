import pymysql
from typing import List, Tuple, Any, Optional
import logging
import time
from tqdm import tqdm
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class MySQLBatchProcessor:
    """
    MySQL批量数据处理工具类
    支持批量插入、更新、删除等操作
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,charset: str = 'utf8mb4',
                 auto_optimize: bool = False):
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

    def batch_execute(self, sql: str, data_list: List[Tuple[Any]], batch_size: int = 1000, show_progress: bool = False,
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

    def _batch_execute_single_threaded(self, sql: str, data_list: List[Tuple[Any]],batch_size: int,
                                       show_progress: bool) -> bool:
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

    def _batch_execute_multithreaded(self, sql: str, data_list: List[Tuple[Any]],batch_size: int, show_progress: bool,
                                     max_workers: int) -> bool:
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

    def batch_insert(self, table_name: str, columns: List[str],data_list: List[Tuple[Any]], batch_size: int = 1000,
                     show_progress: bool = False, use_multithreading: bool = False,max_workers: int = 4) -> bool:
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

    def batch_update(self, table_name: str, set_columns: List[str], where_column: str, data_list: List[Tuple[Any]],
                     batch_size: int = 1000, show_progress: bool = False,use_multithreading: bool = False,
                     max_workers: int = 4) -> bool:
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

    def batch_insert_with_relationships(self, table_configs: List[dict], batch_size: int = 1000,
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


class UniversalBatchInserter(MySQLBatchProcessor):
    """
    通用的批量插入工具类
    继承自MySQLBatchProcessor，专注于处理多表关联数据的批量插入
    在同一事务中处理批次数据，保障数据一致性
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,charset: str = 'utf8mb4',
                 auto_optimize: bool = False):
        """
        初始化

        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 用户名
            password: 密码
            database: 数据库名
            charset: 字符集
            auto_optimize: 是否自动应用批量操作优化设置
        """
        super().__init__(host, port, user, password, database, charset, auto_optimize)

    def batch_insert_related_tables(self, table_data_configs: List[dict], batch_size: int = 1000,
                                    show_progress: bool = True) -> bool:
        """
        批量插入关联表数据（保障事务一致性）

        Args:
            table_data_configs: 表数据配置列表
                每个配置包含:
                - table_name: 表名
                - columns: 列名列表
                - data: 数据列表
                - dependencies: 依赖关系配置 (可选)
                - batch_size: 该表的批量大小 (可选，默认使用全局batch_size)
            batch_size: 默认批量大小
            show_progress: 是否显示进度条

        Returns:
            bool: 插入是否成功
        """
        if not self.connect():
            logging.error("数据库连接失败")
            return False

        cursor = self.connection.cursor()
        progress_bar = None
        try:
            # 按依赖顺序处理表
            ordered_configs = self._order_table_configs_by_dependency(table_data_configs)

            # 计算总的处理记录数
            total_records = sum(len(config['data']) for config in table_data_configs)
            progress_bar = tqdm(total=total_records, desc="批量插入进度", disable=not show_progress,
                                ncols=100, leave=False)

            # 计算每个表的最大批次数量
            table_batches = {}
            for config in ordered_configs:
                table_batch_size = config.get('batch_size', batch_size)  # 使用表特定的批量大小或默认值
                data_count = len(config['data'])
                table_max_batches = (data_count + table_batch_size - 1) // table_batch_size
                table_batches[config['table_name']] = {
                    'max_batches': table_max_batches,
                    'batch_size': table_batch_size,
                    'data': config['data']
                }

            # 计算全局最大批次数量
            max_batches = max([info['max_batches'] for info in table_batches.values()]) if table_batches else 0

            # 按批次处理所有表的数据（跨表批次）
            processed_records = 0
            for batch_idx in range(max_batches):
                # 开始事务
                self.connection.begin()

                try:
                    batch_total_records = 0
                    # 对每个表处理当前批次的数据
                    for config in ordered_configs:
                        table_name = config['table_name']
                        columns = config['columns']
                        table_batch_info = table_batches[table_name]
                        table_batch_size = table_batch_info['batch_size']
                        table_data = table_batch_info['data']

                        # 计算当前批次的数据范围
                        start_idx = batch_idx * table_batch_size
                        end_idx = min((batch_idx + 1) * table_batch_size, len(table_data))

                        if start_idx < len(table_data):  # 如果还有数据
                            batch_data = table_data[start_idx:end_idx]

                            # 执行批量插入
                            sql = self._build_insert_sql(table_name, columns)
                            cursor.executemany(sql, batch_data)
                            batch_total_records += len(batch_data)

                            logging.debug(f"批次 {batch_idx + 1}: 插入表 {table_name} {len(batch_data)} 条记录")

                    # 提交事务（包含所有表的当前批次数据）
                    self.connection.commit()
                    processed_records += batch_total_records
                    progress_bar.update(batch_total_records)

                    logging.info(f"批次 {batch_idx + 1} 插入成功，共 {batch_total_records} 条记录")

                except Exception as e:
                    # 回滚事务
                    self.connection.rollback()
                    logging.error(f"批次 {batch_idx + 1} 插入失败，已回滚: {e}")
                    if progress_bar:
                        progress_bar.close()
                    return False

            if progress_bar:
                progress_bar.close()
            logging.info("所有表数据插入成功")
            return True

        except Exception as e:
            logging.error(f"批量插入关联表数据失败: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if progress_bar:
                progress_bar.close()
            cursor.close()


    def batch_insert_related_tables_advanced(self, table_data_configs: List[dict],base_batch_size: int = 1000,
                                             show_progress: bool = True) -> bool:
        """
        优化后批量插入关联表数据

        Args:
            table_data_configs: 表数据配置列表，每个配置可包含:
                - table_name: 表名
                - columns: 列名列表
                - data: 数据列表
                - dependencies: 依赖关系配置
                - batch_size: 该表的批量大小 (可选)
                - relationship_multiplier: 关系乘数，用于自动计算批次大小 (可选)
            base_batch_size: 基础批量大小
            show_progress: 是否显示进度条

        Returns:
            bool: 插入是否成功
        """
        if not self.connect():
            logging.error("数据库连接失败")
            return False

        cursor = self.connection.cursor()
        progress_bar = None
        try:
            # 按依赖顺序处理表
            ordered_configs = self._order_table_configs_by_dependency(table_data_configs)

            # 计算总的处理记录数
            total_records = sum(len(config['data']) for config in table_data_configs)
            progress_bar = tqdm(total=total_records, desc="批量插入进度", disable=not show_progress,
                                ncols=100, leave=False)

            # 为每个表计算实际的批次大小
            table_configs_with_batch = []
            for config in ordered_configs:
                # 优先使用配置的批次大小，否则根据关系乘数计算
                if 'batch_size' in config:
                    actual_batch_size = config['batch_size']
                elif 'relationship_multiplier' in config:
                    # 根据关系乘数计算批次大小
                    multiplier = config['relationship_multiplier']
                    actual_batch_size = int(base_batch_size * multiplier)
                else:
                    actual_batch_size = base_batch_size

                table_configs_with_batch.append({
                    **config,
                    'actual_batch_size': actual_batch_size
                })

            # 计算每个表的最大批次数量
            table_batches = {}
            for config in table_configs_with_batch:
                table_batch_size = config['actual_batch_size']
                data_count = len(config['data'])
                table_max_batches = (data_count + table_batch_size - 1) // table_batch_size
                table_batches[config['table_name']] = {
                    'max_batches': table_max_batches,
                    'batch_size': table_batch_size,
                    'data': config['data']
                }

            # 计算全局最大批次数量
            max_batches = max([info['max_batches'] for info in table_batches.values()]) if table_batches else 0

            # 按批次处理所有表的数据（跨表批次）
            processed_records = 0
            for batch_idx in range(max_batches):
                # 开始事务
                self.connection.begin()

                try:
                    batch_total_records = 0
                    # 对每个表处理当前批次的数据
                    for config in table_configs_with_batch:
                        table_name = config['table_name']
                        columns = config['columns']
                        table_batch_info = table_batches[table_name]
                        table_batch_size = table_batch_info['batch_size']
                        table_data = table_batch_info['data']

                        # 计算当前批次的数据范围
                        start_idx = batch_idx * table_batch_size
                        end_idx = min((batch_idx + 1) * table_batch_size, len(table_data))

                        if start_idx < len(table_data):  # 如果还有数据
                            batch_data = table_data[start_idx:end_idx]

                            # 执行批量插入
                            sql = self._build_insert_sql(table_name, columns)
                            cursor.executemany(sql, batch_data)
                            batch_total_records += len(batch_data)

                            logging.debug(f"批次 {batch_idx + 1}: 插入表 {table_name} {len(batch_data)} 条记录")

                    # 提交事务（包含所有表的当前批次数据）
                    self.connection.commit()
                    processed_records += batch_total_records
                    progress_bar.update(batch_total_records)

                    logging.info(f"批次 {batch_idx + 1} 插入成功，共 {batch_total_records} 条记录")

                except Exception as e:
                    # 回滚事务
                    self.connection.rollback()
                    logging.error(f"批次 {batch_idx + 1} 插入失败，已回滚: {e}")
                    if progress_bar:
                        progress_bar.close()
                    return False

            if progress_bar:
                progress_bar.close()
            logging.info("所有表数据插入成功")
            return True

        except Exception as e:
            logging.error(f"批量插入关联表数据失败: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if progress_bar:
                progress_bar.close()
            cursor.close()


    def _build_insert_sql(self, table_name: str, columns: List[str]) -> str:
        """
        构建INSERT SQL语句

        Args:
            table_name: 表名
            columns: 列名列表

        Returns:
            str: INSERT SQL语句
        """
        columns_str = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        return f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

    def _order_table_configs_by_dependency(self, table_configs: List[dict]) -> List[dict]:
        """
        根据依赖关系对表配置进行排序

        Args:
            table_configs: 表配置列表

        Returns:
            List[dict]: 按依赖顺序排序的表配置列表
        """
        # 构建依赖图
        dependencies = {}
        config_map = {config['table_name']: config for config in table_configs}

        for config in table_configs:
            table_name = config['table_name']
            dependencies[table_name] = []

            # 检查依赖关系
            if 'dependencies' in config:
                for dep in config['dependencies']:
                    dependencies[table_name].append(dep['parent_table'])

        # 拓扑排序
        visited = set()
        order = []

        def dfs(table_name):
            if table_name in visited:
                return
            visited.add(table_name)

            for dep in dependencies.get(table_name, []):
                dfs(dep)

            order.append(config_map[table_name])

        for table_name in dependencies:
            dfs(table_name)

        return order


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
