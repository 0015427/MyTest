from MySQLScript import MySQLBatchProcessor
from typing import List, Tuple, Any
import logging
import json, os

class MultiTableBatchProcessor(MySQLBatchProcessor):
    """
    扩展的多表批量处理器，专门处理有关联关系的多表数据插入
    """

    def batch_insert_multiple_tables(self,
                                     table_configs: List[dict],
                                     transaction_timeout: int = 3600) -> bool:
        """
        在同一事务中批量插入多个有关联关系的表数据

        Args:
            table_configs: 包含各表配置信息的列表
            transaction_timeout: 事务超时时间(秒)
        """
        if not self.connect():
            return False

        cursor = self.connection.cursor()
        original_autocommit = None

        try:
            # 保存原始autocommit设置
            cursor.execute("SELECT @@autocommit")
            original_autocommit = cursor.fetchone()[0]

            # 开启事务
            self.connection.begin()
            cursor.execute(f"SET SESSION innodb_lock_wait_timeout = {transaction_timeout}")

            # 按依赖顺序依次插入各表数据
            id_mapping = {}  # 存储插入记录的ID映射
            for config in table_configs:
                self._insert_single_table_with_mapping(cursor, config, id_mapping)

            # 提交事务
            self.connection.commit()
            logging.info("多表数据插入成功")
            return True

        except Exception as e:
            # 回滚事务
            self.connection.rollback()
            logging.error(f"多表数据插入失败，已回滚: {e}")
            return False
        finally:
            # 恢复原始设置
            if original_autocommit is not None:
                cursor.execute(f"SET autocommit = {original_autocommit}")
            cursor.close()

    def _insert_single_table_with_mapping(self, cursor, table_config: dict, id_mapping: dict):
        """
        插入单个表的数据，并维护ID映射关系
        """
        table_name = table_config['table_name']
        columns = table_config['columns']
        data_list = table_config['data_list']
        id_field = table_config.get('id_field', 'id')
        mapping_key = table_config.get('mapping_key')  # 用于建立映射关系的字段

        # 处理外键引用
        processed_data = self._process_foreign_keys(data_list, columns, id_mapping, table_config)

        # 构造INSERT语句
        columns_str = ', '.join([f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        # 批量插入数据
        batch_size = table_config.get('batch_size', 1000)
        for i in range(0, len(processed_data), batch_size):
            batch_data = processed_data[i:i + batch_size]
            cursor.executemany(sql, batch_data)

        # 如果需要维护ID映射，则查询刚插入的记录
        if mapping_key:
            self._update_id_mapping(cursor, table_name, mapping_key, id_field, id_mapping, data_list)

    def _process_foreign_keys(self, data_list: List[Tuple], columns: List[str],
                              id_mapping: dict, table_config: dict) -> List[Tuple]:
        """
        处理外键引用，将逻辑键替换为实际ID
        """
        fk_references = table_config.get('foreign_keys', {})
        processed_data = []

        for row in data_list:
            new_row = list(row)
            for col_idx, column in enumerate(columns):
                if column in fk_references:
                    ref_table, ref_key_field = fk_references[column]
                    logical_key = row[col_idx]
                    # 查找实际ID
                    actual_id = id_mapping.get(ref_table, {}).get(logical_key)
                    if actual_id is None:
                        raise ValueError(f"找不到{ref_table}表中{logical_key}对应的ID")
                    new_row[col_idx] = actual_id
            processed_data.append(tuple(new_row))

        return processed_data

    def _update_id_mapping(self, cursor, table_name: str, mapping_key: str,
                           id_field: str, id_mapping: dict, original_data: List[Tuple]):
        """
        更新ID映射关系
        """
        if table_name not in id_mapping:
            id_mapping[table_name] = {}

        # 查询刚插入记录的ID和映射键
        key_indices = []
        for row in original_data:
            key_indices.append(row[0])  # 假设mapping_key是第一列

        # 构造查询语句获取ID
        key_list = "','".join(key_indices)
        query_sql = f"""
        SELECT `{id_field}`, `{mapping_key}` 
        FROM `{table_name}` 
        WHERE `{mapping_key}` IN ('{key_list}')
        """

        cursor.execute(query_sql)
        results = cursor.fetchall()

        for record_id, mapping_value in results:
            id_mapping[table_name][mapping_value] = record_id


def insert_user_order_data_example(processor: MultiTableBatchProcessor):
    """
    用户订单系统的多表数据插入示例
    """
    # 准备数据
    users_data = [
        ('user001', '张三', 'zhangsan@example.com'),
        ('user002', '李四', 'lisi@example.com')
    ]

    orders_data = [
        ('order001', 'user001', 299.99),
        ('order002', 'user001', 199.99),
        ('order003', 'user002', 399.99)
    ]

    order_items_data = [
        ('item001', 'order001', '商品A', 1, 199.99),
        ('item002', 'order001', '商品B', 1, 100.00),
        ('item003', 'order002', '商品C', 2, 99.99),
        ('item004', 'order003', '商品D', 1, 399.99)
    ]

    # 定义表配置
    table_configs = [
        {
            'table_name': 'users',
            'columns': ['user_code', 'name', 'email'],
            'data_list': users_data,
            'mapping_key': 'user_code',  # 用于建立ID映射的字段
            'id_field': 'id'
        },
        {
            'table_name': 'orders',
            'columns': ['order_code', 'user_code', 'amount'],
            'data_list': orders_data,
            'mapping_key': 'order_code',
            'id_field': 'id',
            'foreign_keys': {
                'user_code': ('users', 'user_code')  # 字段名: (引用表名, 引用字段)
            }
        },
        {
            'table_name': 'order_items',
            'columns': ['item_code', 'order_code', 'product_name', 'quantity', 'price'],
            'data_list': order_items_data,
            'foreign_keys': {
                'order_code': ('orders', 'order_code')
            }
        }
    ]

    # 执行多表插入
    success = processor.batch_insert_multiple_tables(table_configs)
    return success


class ResilientMultiTableProcessor(MultiTableBatchProcessor):
    """
    具有容错能力的多表处理器
    """

    def batch_insert_with_checkpoint(self,
                                     table_configs: List[dict],
                                     checkpoint_file: str = "insert_checkpoint.json") -> bool:
        """
        带断点续传功能的多表插入
        """
        # 检查是否存在断点文件
        checkpoint = self._load_checkpoint(checkpoint_file)

        if not self.connect():
            return False

        cursor = self.connection.cursor()
        try:
            # 开启事务
            self.connection.begin()

            # 从断点继续处理
            start_index = checkpoint.get('completed_tables', 0)
            id_mapping = checkpoint.get('id_mapping', {})

            for i in range(start_index, len(table_configs)):
                config = table_configs[i]
                self._insert_single_table_with_mapping(cursor, config, id_mapping)

                # 更新断点
                checkpoint['completed_tables'] = i + 1
                checkpoint['id_mapping'] = id_mapping
                self._save_checkpoint(checkpoint, checkpoint_file)

            # 提交事务并清理断点文件
            self.connection.commit()
            self._remove_checkpoint(checkpoint_file)
            return True

        except Exception as e:
            self.connection.rollback()
            logging.error(f"插入过程出错: {e}")
            return False
        finally:
            cursor.close()

    def _load_checkpoint(self, checkpoint_file: str) -> dict:
        """加载断点信息"""
        try:
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logging.warning(f"加载断点文件失败: {e}")
        return {'completed_tables': 0, 'id_mapping': {}}

    def _save_checkpoint(self, checkpoint: dict, checkpoint_file: str):
        """保存断点信息"""
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
        except Exception as e:
            logging.warning(f"保存断点文件失败: {e}")

    def _remove_checkpoint(self, checkpoint_file: str):
        """移除断点文件"""
        try:
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
        except Exception as e:
            logging.warning(f"移除断点文件失败: {e}")
