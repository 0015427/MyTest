import unittest

from MySQLScriptV4.ddl import DdlError, ensure_if_not_exists, parse_table


class DdlTest(unittest.TestCase):
    def test_basic(self):
        info = parse_table(
            """
            CREATE TABLE `users` (
              `id` VARCHAR(32) NOT NULL,
              `name` VARCHAR(40) NOT NULL COMMENT 'a,b (c)',
              `country` ENUM('CN','US','JP') NOT NULL DEFAULT 'CN',
              `balance` DECIMAL(12,2) UNSIGNED NOT NULL DEFAULT 0.00,
              `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              UNIQUE KEY uk_name (`name`),
              KEY idx_c (country)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='users, v1';
            """
        )
        self.assertEqual(info.name, "users")
        self.assertEqual(info.primary_key, "id")
        self.assertEqual(info.column_names, ("id", "name", "country", "balance", "created_at"))
        country = info.column("country")
        self.assertEqual(country.kind, "enum")
        self.assertEqual(country.enum_values, ("CN", "US", "JP"))
        balance = info.column("balance")
        self.assertEqual((balance.kind, balance.precision, balance.scale), ("decimal", 12, 2))
        self.assertTrue(balance.unsigned)
        self.assertEqual(info.column("created_at").default_sql, "CURRENT_TIMESTAMP")

    def test_comments_and_reserved_column(self):
        info = parse_table(
            """
            -- a comment with (paren)
            CREATE TABLE t (
              `key` INT NOT NULL,      # reserved word as column
              v VARCHAR(10) DEFAULT 'x--y',
              PRIMARY KEY (`key`)
            );
            """
        )
        self.assertEqual(info.column_names, ("key", "v"))
        self.assertEqual(info.column("v").default_sql, "'x--y'")

    def test_double_quotes(self):
        info = parse_table('CREATE TABLE "orders" ("id" BIGINT PRIMARY KEY, "created_at" TIMESTAMP NULL)')
        self.assertEqual(info.primary_key, "id")
        self.assertFalse(info.column("created_at").not_null)

    def test_generated_column_excluded(self):
        info = parse_table(
            """
            CREATE TABLE g (
              a INT NOT NULL,
              b INT NOT NULL,
              total INT GENERATED ALWAYS AS (a + b) STORED,
              PRIMARY KEY (a)
            )
            """
        )
        self.assertTrue(info.column("total").generated)
        self.assertEqual([c.name for c in info.insert_columns], ["a", "b"])

    def test_composite_pk_rejected(self):
        with self.assertRaises(DdlError) as ctx:
            parse_table("CREATE TABLE c (a INT, b INT, PRIMARY KEY (a, b))")
        self.assertIn("单列主键", str(ctx.exception))

    def test_name_mismatch(self):
        with self.assertRaises(DdlError):
            parse_table("CREATE TABLE a (id INT PRIMARY KEY)", "b")

    def test_special_ident_rejected(self):
        with self.assertRaises(DdlError):
            parse_table("CREATE TABLE `my table` (id INT)")

    def test_no_pk_allowed(self):
        info = parse_table("CREATE TABLE n (a INT, b TEXT)")
        self.assertIsNone(info.primary_key)
        self.assertEqual(info.column("b").kind, "text")

    def test_ensure_if_not_exists(self):
        self.assertIn("IF NOT EXISTS", ensure_if_not_exists("CREATE TABLE t (id INT)"))
        already = "CREATE TABLE IF NOT EXISTS t (id INT)"
        self.assertEqual(ensure_if_not_exists(already), already)
        self.assertIn("IF NOT EXISTS", ensure_if_not_exists("CREATE TEMPORARY TABLE t (id INT)"))

    def test_missing_type(self):
        with self.assertRaises(DdlError):
            parse_table("CREATE TABLE t (id)")

    def test_double_quoted_pk(self):
        info = parse_table('CREATE TABLE t (id BIGINT, v TEXT, PRIMARY KEY ("id"))')
        self.assertEqual(info.primary_key, "id")


if __name__ == "__main__":
    unittest.main()
